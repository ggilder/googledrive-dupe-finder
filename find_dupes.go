package main

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sort"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/dustin/go-humanize/english"
	"github.com/jessevdk/go-flags"
	"github.com/mitchellh/go-homedir"

	"golang.org/x/text/unicode/norm"

	"google.golang.org/api/drive/v3"
)

// File stores the result of either API or local file listing
type File struct {
	Path        string
	Size        int64
	ContentHash string
}

type RemoteManifest map[string][]*File

type scanProgressUpdate struct {
	Count int
}

type Duplication struct {
	ContentHash    string
	Files          []*File
	DuplicateCount int
	DuplicateSize  uint64
}

type DuplicateReport struct {
	Duplications        []*Duplication
	TotalDuplicateCount int
	TotalDuplicateSize  uint64
}

func main() {
	homeDir, err := homedir.Dir()
	if err != nil {
		fmt.Fprintln(os.Stderr, "Please set $HOME to a readable path!")
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
	configDir := filepath.Join(homeDir, ".googledrive-sync-verifier")
	srv, err := NewDriveService(filepath.Join(configDir, "credentials.json"), filepath.Join(configDir, "token.json"))

	var opts struct {
		Verbose            bool `short:"v" long:"verbose" description:"Show verbose debug information"`
		FreeMemoryInterval int  `long:"free-memory-interval" description:"Interval (in seconds) to manually release unused memory back to the OS on low-memory systems" default:"0"`
	}

	_, err = flags.Parse(&opts)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	fmt.Printf("Scanning Google Drive for duplicates\n\n")

	progressChan := make(chan *scanProgressUpdate)
	var wg sync.WaitGroup
	wg.Add(1)

	var driveManifest RemoteManifest
	var driveError error
	go func() {
		driveManifest, driveError = getGoogleDriveManifest(progressChan, srv, "/")
		wg.Done()
	}()

	go func() {
		for update := range progressChan {
			if opts.Verbose {
				fmt.Fprintf(os.Stderr, "Scanning: %d files\r", update.Count)
			}
		}
		fmt.Fprintf(os.Stderr, "\n")
	}()

	// set up manual garbage collection routine
	if opts.FreeMemoryInterval > 0 {
		go func() {
			for range time.Tick(time.Duration(opts.FreeMemoryInterval) * time.Second) {
				var m, m2 runtime.MemStats
				if opts.Verbose {
					runtime.ReadMemStats(&m)
				}
				debug.FreeOSMemory()
				if opts.Verbose {
					runtime.ReadMemStats(&m2)
					fmt.Fprintf(
						os.Stderr,
						"\n[%s] Alloc: %s -> %s / Sys: %s -> %s / HeapInuse: %s -> %s / HeapReleased: %s -> %s\n",
						time.Now().Format("15:04:05"),
						humanize.Bytes(m.Alloc),
						humanize.Bytes(m2.Alloc),
						humanize.Bytes(m.Sys),
						humanize.Bytes(m2.Sys),
						humanize.Bytes(m.HeapInuse),
						humanize.Bytes(m2.HeapInuse),
						humanize.Bytes(m.HeapReleased),
						humanize.Bytes(m2.HeapReleased),
					)
				}
			}
		}()
	}

	// wait until scan is complete, then close progress reporting channel
	wg.Wait()
	close(progressChan)
	// TODO figure out why duplicate line of stderr gets printed here
	fmt.Printf("\nFinished scanning.\n\n")

	// check for fatal errors
	if driveError != nil {
		panic(driveError)
	}

	// Analyze results for dupe info
	report := analyzeDuplicates(driveManifest)
	fmt.Printf("%d duplicate file groups found (%d files, %s).\n\n", len(report.Duplications), report.TotalDuplicateCount, humanize.Bytes(report.TotalDuplicateSize))
	group := 1
	for _, duplication := range report.Duplications {
		fmt.Printf(
			"Group %d (%s, %s)\n",
			group,
			english.Plural(duplication.DuplicateCount, "duplicate file", ""),
			humanize.Bytes(duplication.DuplicateSize),
		)
		for _, f := range duplication.Files {
			fmt.Println(f.Path)
		}
		fmt.Println("")
		group++
	}
	fmt.Println("")
}

func analyzeDuplicates(manifest RemoteManifest) (report *DuplicateReport) {
	// TODO (stretch goal) compute hashes of directories to find wholly duplicated directories (before filtering?)
	report = &DuplicateReport{}
	for hash, files := range manifest {
		if len(files) <= 1 {
			continue
		}
		filteredFiles := filterDuplicateFiles(files)
		if len(filteredFiles) <= 1 {
			continue
		}
		duplicateCount := 0
		duplicateSize := uint64(0)
		for idx, f := range filteredFiles {
			// Don't count first file since we still would presumably keep one
			if idx == 0 {
				continue
			}
			duplicateCount++
			// TODO implement some kind of sanity check to make sure all files in a group have the same size?
			duplicateSize += uint64(f.Size)
		}

		report.TotalDuplicateCount += duplicateCount
		report.TotalDuplicateSize += duplicateSize
		report.Duplications = append(report.Duplications, &Duplication{
			ContentHash:    hash,
			Files:          filteredFiles,
			DuplicateCount: duplicateCount,
			DuplicateSize:  duplicateSize,
		})
	}
	// sort duplications by size (descending)
	sort.Slice(report.Duplications, func(i, j int) bool {
		return report.Duplications[i].DuplicateSize >= report.Duplications[j].DuplicateSize
	})
	return
}

func filterDuplicateFiles(files []*File) (filteredFiles []*File) {
	for _, file := range files {
		if !ignoreFile(file) {
			filteredFiles = append(filteredFiles, file)
		}
	}
	return
}

func ignoreFile(file *File) bool {
	// TODO extract config
	if file.Size < 1000 {
		return true
	}
	// TODO filter path (like git files or maybe all dotfiles)
	// .....
	return false
}

func normalizePath(entryPath string) string {
	// Normalize Unicode combining characters
	return norm.NFC.String(entryPath)
}

func getGoogleDriveManifest(progressChan chan<- *scanProgressUpdate, srv *drive.Service, rootPath string) (manifest RemoteManifest, err error) {
	manifest = RemoteManifest{}

	listing := NewDriveListing(srv)
	listing.RootPath = rootPath
	updateChan := make(chan int)
	go func() {
		for updateCount := range updateChan {
			progressChan <- &scanProgressUpdate{Count: updateCount}
		}
	}()
	files, err := listing.Files(updateChan)
	if err != nil {
		return
	}
	for _, file := range files {
		manifest[file.ContentHash] = append(manifest[file.ContentHash], file)
	}

	return manifest, nil
}
