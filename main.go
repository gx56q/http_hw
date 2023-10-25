package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

var githubToken string
var organization string

type Commit struct {
	Author struct {
		Email string `json:"email"`
	} `json:"author"`
	Message string `json:"message"`
}

type Repository struct {
	Name string `json:"name"`
}

var (
	rateLimitMutex sync.Mutex
	rateLimitCond  = sync.NewCond(&rateLimitMutex)
	rateLimited    = false
)

func main() {
	loadConfig()
	client := http.Client{Timeout: time.Second * 30}

	fmt.Println("Getting the list of organization repositories...")
	repositories, err := getRepositories(client, organization)
	if err != nil {
		fmt.Println("Error getting the list of repositories:", err)
		os.Exit(1)
	}
	fmt.Printf("Found %d repositories in the organization %s\n", len(repositories), organization)

	fmt.Println("Getting the list of active authors...")
	authors := make(map[string]int)
	mux := &sync.Mutex{}
	wg := &sync.WaitGroup{}
	sem := make(chan bool, 15)

	totalRepos := len(repositories)
	processedRepos := 0

	for _, repo := range repositories {
		wg.Add(1)
		sem <- true
		go func(r Repository) {
			defer wg.Done()
			defer func() { <-sem }()

			commits, err := getCommits(client, organization, r.Name)
			if err != nil {
				fmt.Printf("Error getting commits for repository %s: %v\n", r.Name, err)
				return
			}
			uniqueAuthors := make(map[string]bool)
			mux.Lock()
			for _, commit := range commits {
				if !strings.HasPrefix(commit.Message, "Merge pull request #") {
					authors[commit.Author.Email]++
					uniqueAuthors[commit.Author.Email] = true
				}
			}
			processedRepos++
			fmt.Printf("Repository %-50s | Processed: %4d | Remaining: %4d\n", r.Name, processedRepos, totalRepos-processedRepos)
			mux.Unlock()
		}(repo)
	}
	wg.Wait()

	fmt.Println("Sorting authors by activity...")
	sortedAuthors := sortAuthors(authors)
	fmt.Println("Top 100 most active authors in the organization", organization, "on GitHub:")
	limit := 100
	if len(sortedAuthors) < limit {
		limit = len(sortedAuthors)
	}
	for i, author := range sortedAuthors[:limit] {
		fmt.Printf("%d. %s: %d\n", i+1, author.Email, author.Commits)
	}
	err = writeAuthorsToFile(organization, len(repositories), sortedAuthors)
	if err != nil {
		fmt.Println("Error writing authors to file:", err)
		os.Exit(1)
	}
}

func doRequest(client http.Client, query string) ([]byte, error) {
	reqBodyData := GraphQLRequest{Query: query}
	reqBody, err := json.Marshal(reqBodyData)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", "https://api.github.com/graphql", bytes.NewBuffer(reqBody))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "bearer "+githubToken)

	rateLimitMutex.Lock()
	for rateLimited {
		rateLimitCond.Wait()
	}
	rateLimitMutex.Unlock()
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			fmt.Println("Error closing response body:", err)
		}
	}(resp.Body)

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode == 403 {
		rateLimitMutex.Lock()
		rateLimited = true
		rateLimitMutex.Unlock()
		bodyString := string(bodyBytes)
		if strings.Contains(bodyString, "secondary rate limit") {
			fmt.Println("Secondary rate limit reached for request. Waiting...")
			retryAfter := resp.Header.Get("Retry-After")
			if retryAfter != "" {
				delay, err := strconv.Atoi(retryAfter)
				if err == nil {
					exactResetTime := time.Now().Add(time.Duration(delay) * time.Second)
					fmt.Printf("Waiting for %d seconds until the limit resets at %s...\n", delay, exactResetTime.Format("15:04:05"))
					time.Sleep(time.Duration(delay) * time.Second)
					fmt.Println("Retrying request...")
					rateLimitMutex.Lock()
					rateLimited = false
					rateLimitCond.Broadcast()
					rateLimitMutex.Unlock()

					return doRequest(client, query)
				}
			} else {
				exactResetTime := time.Now().Add(1 * time.Minute)
				fmt.Printf("Waiting for 1 minute until the limit resets at %s...\n", exactResetTime.Format("15:04:05"))
				time.Sleep(1 * time.Minute)
				fmt.Println("Retrying request...")
				rateLimitMutex.Lock()
				rateLimited = false
				rateLimitCond.Broadcast()
				rateLimitMutex.Unlock()

				return doRequest(client, query)
			}
		}
		return nil, fmt.Errorf("unexpected status code: %d. Body: %s", resp.StatusCode, bodyString)
	}

	remaining := resp.Header.Get("X-RateLimit-Remaining")
	if remaining == "0" {
		resetTime := resp.Header.Get("X-RateLimit-Reset")
		resetTimestamp, err := strconv.ParseInt(resetTime, 10, 64)
		if err == nil {
			resetTimeDuration := time.Until(time.Unix(resetTimestamp, 0))
			exactResetTime := time.Now().Add(resetTimeDuration)
			fmt.Printf("Reached the rate limit. Waiting %v until the limit resets at %s...\n", resetTimeDuration, exactResetTime.Format("15:04:05"))
			time.Sleep(resetTimeDuration)
			return doRequest(client, query)
		}
		return nil, fmt.Errorf("reached GitHub API rate limit")
	}

	return bodyBytes, nil
}

func getRepositories(client http.Client, org string) ([]Repository, error) {
	var allRepos []Repository
	var endCursor *string
	for {
		query := `
{
  organization(login: "%s") {
    repositories(first: 100, after: %s, isFork: false) {
      pageInfo {
        endCursor
        hasNextPage
      }
      nodes {
        name
      }
    }
  }
}
`
		endCursorStr := "null"
		if endCursor != nil {
			endCursorStr = fmt.Sprintf(`"%s"`, *endCursor)
		}
		query = fmt.Sprintf(query, org, endCursorStr)

		repos, hasNextPage, newEndCursor, err := fetchRepositories(client, query)
		if err != nil {
			return nil, err
		}
		allRepos = append(allRepos, repos...)
		if !hasNextPage {
			break
		}
		endCursor = &newEndCursor
	}
	return allRepos, nil
}

type GraphQLRequest struct {
	Query string `json:"query"`
}

func fetchRepositories(client http.Client, query string) ([]Repository, bool, string, error) {
	bodyBytes, err := doRequest(client, query)
	if err != nil {
		return nil, false, "", err
	}

	var response struct {
		Data struct {
			Organization struct {
				Repositories struct {
					PageInfo struct {
						EndCursor   string `json:"endCursor"`
						HasNextPage bool   `json:"hasNextPage"`
					} `json:"pageInfo"`
					Nodes []Repository
				}
			}
		}
		Errors []struct {
			Message string `json:"message"`
		}
	}

	if err := json.Unmarshal(bodyBytes, &response); err != nil {
		return nil, false, "", err
	}

	if len(response.Errors) > 0 {
		return nil, false, "", fmt.Errorf("GraphQL error: %s", response.Errors[0].Message)
	}

	if response.Data.Organization.Repositories.Nodes == nil {
		return nil, false, "", fmt.Errorf("no repositories found for organization")
	}

	return response.Data.Organization.Repositories.Nodes, response.Data.Organization.Repositories.PageInfo.HasNextPage, response.Data.Organization.Repositories.PageInfo.EndCursor, nil
}

func getDefaultBranch(client http.Client, org, repo string) (string, error) {
	query := `
{
  repository(owner: "%s", name: "%s") {
    defaultBranchRef {
      name
    }
  }
}
`
	query = fmt.Sprintf(query, org, repo)
	reqBodyData := GraphQLRequest{
		Query: query,
	}
	reqBody, err := json.Marshal(reqBodyData)
	if err != nil {
		return "", err
	}

	req, err := http.NewRequest("POST", "https://api.github.com/graphql", bytes.NewBuffer(reqBody))
	req.Header.Set("Authorization", "bearer "+githubToken)

	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			fmt.Println("Error closing response body:", err)
		}
	}(resp.Body)

	var response struct {
		Data struct {
			Repository struct {
				DefaultBranchRef struct {
					Name string `json:"name"`
				} `json:"defaultBranchRef"`
			} `json:"repository"`
		}
	}
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return "", err
	}

	return response.Data.Repository.DefaultBranchRef.Name, nil
}

func getCommits(client http.Client, org, repo string) ([]Commit, error) {
	var allCommits []Commit
	var endCursor *string

	branchName, err := getDefaultBranch(client, org, repo)
	if err != nil {
		return nil, fmt.Errorf("error getting default branch: %v", err)
	}

	for {
		query := `
{
  repository(owner: "%s", name: "%s") {
    ref(qualifiedName: "%s") {
      target {
        ... on Commit {
          history(first: 100, after: %s) {
            pageInfo {
              endCursor
              hasNextPage
            }
            nodes {
              message
              author {
                email
              }
            }
          }
        }
      }
    }
  }
}
`
		endCursorStr := "null"
		if endCursor != nil {
			endCursorStr = fmt.Sprintf(`"%s"`, *endCursor)
		}
		query = fmt.Sprintf(query, org, repo, branchName, endCursorStr)

		commits, hasNextPage, newEndCursor, err := fetchCommits(client, query)
		if err != nil {
			fmt.Println("Error getting commits for repository:", repo, err)
			return nil, err
		}
		allCommits = append(allCommits, commits...)
		if !hasNextPage {
			break
		}
		endCursor = &newEndCursor
	}
	return allCommits, nil
}

func fetchCommits(client http.Client, query string) ([]Commit, bool, string, error) {
	bodyBytes, err := doRequest(client, query)
	if err != nil {
		return nil, false, "", err
	}

	var response struct {
		Data struct {
			Repository struct {
				Ref struct {
					Target struct {
						History struct {
							PageInfo struct {
								EndCursor   string `json:"endCursor"`
								HasNextPage bool   `json:"hasNextPage"`
							} `json:"pageInfo"`
							Nodes []Commit `json:"nodes"`
						} `json:"history"`
					} `json:"target"`
				} `json:"ref"`
			} `json:"repository"`
		} `json:"data"`
	}

	if err := json.Unmarshal(bodyBytes, &response); err != nil {
		return nil, false, "", err
	}

	return response.Data.Repository.Ref.Target.History.Nodes, response.Data.Repository.Ref.Target.History.PageInfo.HasNextPage, response.Data.Repository.Ref.Target.History.PageInfo.EndCursor, nil
}

type Author struct {
	Email   string
	Commits int
}

func sortAuthors(authors map[string]int) []Author {
	sortedAuthors := make([]Author, 0, len(authors))
	for email, commits := range authors {
		sortedAuthors = append(sortedAuthors, Author{Email: email, Commits: commits})
	}
	sortSlice := func(i, j int) bool {
		return sortedAuthors[i].Commits > sortedAuthors[j].Commits
	}
	sort.Slice(sortedAuthors, sortSlice)
	return sortedAuthors
}

func writeAuthorsToFile(org string, numOfRepos int, sortedAuthors []Author) error {
	filename := fmt.Sprintf("%s_%d.txt", org, numOfRepos)
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			fmt.Println("Error closing file:", err)
		}
	}(file)

	for i, author := range sortedAuthors[:100] {
		line := fmt.Sprintf("%d. %s: %d\n", i+1, author.Email, author.Commits)
		_, err := file.WriteString(line)
		if err != nil {
			return err
		}
	}
	return nil
}

func loadConfig() {
	file, err := os.Open("config.txt")
	if err != nil {
		panic("Failed to open the configuration file: " + err.Error())
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			panic("Failed to close the configuration file: " + err.Error())
		}
	}(file)

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		if githubToken == "" {
			githubToken = scanner.Text()
		} else {
			organization = scanner.Text()
			break
		}
	}

	if err := scanner.Err(); err != nil {
		panic("Error reading the configuration file: " + err.Error())
	}
}