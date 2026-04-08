package adapter

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"log"
	pathpkg "path"
	"strings"
	"time"

	"github.com/openwebui-content-sync/internal/config"
	"github.com/sirupsen/logrus"
	gitlab "gitlab.com/gitlab-org/api/client-go"
)

// GitLabAdapter implements the Adapter interface for GitLab repositories
type GitLabAdapter struct {
	client       *gitlab.Client
	config       config.GitLabConfig
	lastSync     time.Time
	repositories []string
	mappings     map[string]mappingInfo // repository -> mappingInfo (knowledge_id, branch)
}

type mappingInfo struct {
	KnowledgeID string
	Branch      string
}

// NewGitLabAdapter creates a new GitLab adapter
func NewGitLabAdapter(cfg config.GitLabConfig) (*GitLabAdapter, error) {
	if cfg.Token == "" {
		return nil, fmt.Errorf("GitLab token is required")
	}

	git, err := gitlab.NewClient(cfg.Token, gitlab.WithBaseURL(cfg.BaseURL))
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	// Build repository mappings
	mappings := make(map[string]mappingInfo)
	repos := []string{}

	// Process mappings
	for _, mapping := range cfg.Mappings {
		if mapping.Repository != "" && mapping.KnowledgeID != "" {
			mappings[mapping.Repository] = mappingInfo{
				KnowledgeID: mapping.KnowledgeID,
				Branch:      mapping.Branch,
			}
			repos = append(repos, mapping.Repository)
		}
	}

	if len(repos) == 0 {
		return nil, fmt.Errorf("at least one repository mapping must be configured")
	}

	return &GitLabAdapter{
		client:       git,
		config:       cfg,
		repositories: repos,
		mappings:     mappings,
		lastSync:     time.Now().Add(-24 * time.Hour), // Default to 24 hours ago
	}, nil

}

// Name returns the adapter name
func (g *GitLabAdapter) Name() string {
	return "gitlab"
}

// FetchFiles implements [Adapter].
func (g *GitLabAdapter) FetchFiles(ctx context.Context) ([]*File, error) {
	var files []*File

	for _, repo := range g.repositories {
		logrus.Debugf("Fetching files from repository: %s", repo)
		mapping := g.mappings[repo]
		repoFiles, err := g.fetchRepositoryFiles(ctx, repo, mapping)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch files from repository %s: %w", repo, err)
		}
		logrus.Debugf("Found %d files in repository %s (knowledge_id: %s, branch: %s)", len(repoFiles), repo, mapping.KnowledgeID, mapping.Branch)
		files = append(files, repoFiles...)
	}

	logrus.Debugf("Total files fetched: %d", len(files))
	return files, nil
}

// fetchRepositoryFiles fetches files from a specific repository
func (g *GitLabAdapter) fetchRepositoryFiles(ctx context.Context, repo string, mapping mappingInfo) ([]*File, error) {
	parts := strings.Split(repo, "/")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid repository format, expected 'owner/repo'")
	}

	// Get repository contents
	opts := &gitlab.ListTreeOptions{}
	if mapping.Branch != "" {
		opts.Ref = gitlab.Ptr(mapping.Branch)
	}
	tree, _, err := g.client.Repositories.ListTree(repo, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to get repository tree: %w", err)
	}

	var files []*File
	for _, content := range tree {
		fileList, err := g.processContent(ctx, repo, mapping.Branch, content, "", mapping.KnowledgeID)
		if err != nil {
			continue // Skip files that can't be processed
		}
		if fileList != nil {
			files = append(files, fileList...)
		}
	}

	return files, nil
}

// processContent processes a GitLab content item recursively
func (g *GitLabAdapter) processContent(ctx context.Context, repo, branch string, content *gitlab.TreeNode, path string, knowledgeID string) ([]*File, error) {
	if content == nil {
		return nil, nil
	}

	// Correction : bien gérer le chemin courant avec path.Join pour GitLab
	var currentPath string
	if path == "" {
		currentPath = content.Name
	} else {
		currentPath = pathpkg.Join(path, content.Name)
	}

	if content.Type == "blob" {
		if !isTextFile(content.Name) {
			return nil, nil
		}
		fileContent, lastModified, err := g.getFileContent(ctx, repo, currentPath, branch)
		if err != nil {
			return nil, fmt.Errorf("failed to get file content: %w", err)
		}
		hash := fmt.Sprintf("%x", sha256.Sum256(fileContent))
		return []*File{{
			Path:        currentPath,
			Content:     fileContent,
			Hash:        hash,
			Modified:    lastModified,
			Size:        int64(len(fileContent)),
			Source:      fmt.Sprintf("%s", repo),
			KnowledgeID: knowledgeID,
		}}, nil
	}

	if content.Type == "tree" {
		opts := &gitlab.ListTreeOptions{
			Path: gitlab.Ptr(currentPath),
		}
		if branch != "" {
			opts.Ref = gitlab.Ptr(branch)
		}
		tree, _, err := g.client.Repositories.ListTree(repo, opts)
		if err != nil {
			return nil, fmt.Errorf("failed to get repository tree: %w", err)
		}

		var allFiles []*File
		for _, subContent := range tree {
			files, err := g.processContent(ctx, repo, branch, subContent, currentPath, knowledgeID)
			if err != nil {
				continue
			}
			if files != nil {
				allFiles = append(allFiles, files...)
			}
		}
		return allFiles, nil
	}

	return nil, nil
}

// getFileContent retrieves the actual content of a file
func (g *GitLabAdapter) getFileContent(ctx context.Context, repo, path string, branch string) (content []byte, lastModified time.Time, err error) {
	opts := &gitlab.GetFileOptions{}
	if branch != "" {
		opts.Ref = gitlab.Ptr(branch)
	}

	file, _, err := g.client.RepositoryFiles.GetFile(repo, path, opts)
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("failed to get content: %w", err)
	}

	if file.FileName != "" && file.Content != "" {
		// Le contenu est encodé en base64 par l'API GitLab
		decoded, err := base64.StdEncoding.DecodeString(file.Content)
		if err != nil {
			return nil, time.Time{}, fmt.Errorf("failed to decode base64 content: %w", err)
		}
		lastModified, _ := g.getFileLastModified(ctx, repo, file.LastCommitID)
		return decoded, lastModified, nil
	}

	return nil, time.Time{}, fmt.Errorf("file content is not available for file: %s", path)
}

func (g *GitLabAdapter) getFileLastModified(ctx context.Context, repo, lastCommitID string) (time.Time, error) {
	commit, _, err := g.client.Commits.GetCommit(repo, lastCommitID, nil)
	if err != nil {
		return time.Time{}, fmt.Errorf("error while getting commit: %w", err)
	}

	return *commit.CommittedDate, nil
}

// GetLastSync implements [Adapter].
func (g *GitLabAdapter) GetLastSync() time.Time {
	return g.lastSync
}

// SetLastSync implements [Adapter].
func (g *GitLabAdapter) SetLastSync(t time.Time) {
	g.lastSync = t
}
