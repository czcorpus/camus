package kcache

import (
	"camus/cncdb"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/rs/zerolog/log"
)

// ------------------------

type statsRecord struct {
	Corpus        string  `json:"corpus"`
	CorpusSize    int64   `json:"corpusSize"`
	SubcorpusSize int64   `json:"subcorpusSize"`
	TimeProc      float64 `json:"timeProc"`
	Query         string  `json:"query"`
}

// -----------------------

// Meter is a service which processes incoming CorpBoundRawRecord records
// (= QueryArchRec and corpus ID together) and stores them to a file
// for later processing by other tools (CQLizer).
type Meter struct {
	cacheReader   *CacheReader
	incoming      <-chan cncdb.CorpBoundRawRecord
	statsFile     *os.File
	statsFilePath string
	// MaxFileSize is the maximum size in bytes before rotating the file
	// Default: 10MB
	MaxFileSize int64
	ctx         context.Context
	done        chan struct{}
}

func (meter *Meter) writeStats(rec *statsRecord) error {
	// Skip writing in dummy mode (empty statsFilePath)
	if meter.statsFilePath == "" {
		return nil
	}

	// Check if file needs rotation
	if meter.statsFile != nil {
		fileInfo, err := meter.statsFile.Stat()
		if err != nil {
			return fmt.Errorf("failed to stat stats file: %w", err)
		}

		if fileInfo.Size() >= meter.MaxFileSize {
			if err := meter.rotateStatsFile(); err != nil {
				return fmt.Errorf("failed to prepare stats file: %w", err)
			}
		}
	}

	// Ensure file is open
	if meter.statsFile == nil {
		file, err := os.OpenFile(meter.statsFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return fmt.Errorf("failed to open stats file: %w", err)
		}
		meter.statsFile = file
	}

	// Marshal record to JSON and append as JSONL
	data, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("failed to marshal stats record: %w", err)
	}

	// Append newline for JSONL format
	data = append(data, '\n')

	if _, err := meter.statsFile.Write(data); err != nil {
		return fmt.Errorf("failed to write stats record: %w", err)
	}

	return nil
}

func (meter *Meter) rotateStatsFile() error {
	if meter.statsFile != nil {
		if err := meter.statsFile.Close(); err != nil {
			return fmt.Errorf("failed to close stats file during rotation: %w", err)
		}
		meter.statsFile = nil
	}

	// Create rotation filename with date suffix
	dateStr := time.Now().Format("2006-01-02")
	rotatedPath := fmt.Sprintf("%s-%s", meter.statsFilePath, dateStr)

	// Rename current file
	if err := os.Rename(meter.statsFilePath, rotatedPath); err != nil {
		return fmt.Errorf("failed to rate stats file: %w", err)
	}

	log.Info().
		Str("from", meter.statsFilePath).
		Str("to", rotatedPath).
		Msg("rotated stats file")

	return nil
}

func (meter *Meter) listenForData() {
	defer meter.cleanup()
	defer close(meter.done)

	for {
		select {
		case <-meter.ctx.Done():
			log.Info().Msg("meter context cancelled, shutting down")
			return
		case item, ok := <-meter.incoming:
			if !ok {
				log.Info().Msg("meter incoming channel closed, shutting down")
				return
			}
			rec, err := meter.cacheReader.GetConcCacheRecordByConcID(item.ID())
			if err != nil {
				log.Error().Err(err).Str("concId", item.ID()).Msg("failed to get conc archive record")
				continue
			}
			arch, err := item.FetchData()
			if err != nil {
				log.Error().Err(err).Str("concId", item.ID()).Msg("failed to extract a query from the archive record")
				continue
			}
			qChain := arch.GetQuery()
			if len(qChain) == 0 {
				log.Error().Err(fmt.Errorf("missing query")).Str("concId", item.ID()).Msg("failed to process archived query")
				continue
			}
			if rec.IsProcessable() {
				sr := &statsRecord{
					Corpus:     item.Corpname,
					CorpusSize: item.CorpusSize,
					TimeProc:   rec.ProcTime(),
					Query:      qChain[0],
				}
				if err := meter.writeStats(sr); err != nil {
					log.Error().Err(err).Str("concId", item.ID()).Msg("failed to write stats data")
					continue
				}
			}
		}
	}
}

func (meter *Meter) cleanup() {
	if meter.statsFile != nil {
		if err := meter.statsFile.Close(); err != nil {
			log.Error().Err(err).Msg("failed to close stats file during cleanup")
		} else {
			log.Info().Str("path", meter.statsFilePath).Msg("closed stats file")
		}
		meter.statsFile = nil
	}
}

// Start implements the service interface
func (meter *Meter) Start(ctx context.Context) {
	if meter.statsFilePath == "" {
		log.Info().Msg("starting meter service in dummy mode (no stats will be written)")
	} else {
		log.Info().Str("statsPath", meter.statsFilePath).Msg("starting meter service")
	}
	go meter.listenForData()
}

// Stop implements the service interface
func (meter *Meter) Stop(ctx context.Context) error {
	log.Info().Msg("stopping meter service")

	// Wait for listenForData to finish or context timeout
	select {
	case <-meter.done:
		log.Info().Msg("meter service stopped gracefully")
		return nil
	case <-ctx.Done():
		log.Warn().Msg("meter service stop timed out")
		return ctx.Err()
	}
}

func NewMeter(ctx context.Context, statsPath string, cache *CacheReader, incData <-chan cncdb.CorpBoundRawRecord) (*Meter, error) {
	// Check if dummy mode (empty statsPath means no writing)
	dummyMode := statsPath == ""

	// If not in dummy mode, perform sanity checks
	if !dummyMode {
		// Sanity check: verify directory exists and is writable
		dir := filepath.Dir(statsPath)

		// Check if directory exists
		dirInfo, err := os.Stat(dir)
		if err != nil {
			if os.IsNotExist(err) {
				return nil, fmt.Errorf("directory does not exist: %s", dir)
			}
			return nil, fmt.Errorf("failed to stat directory %s: %w", dir, err)
		}

		if !dirInfo.IsDir() {
			return nil, fmt.Errorf("path is not a directory: %s", dir)
		}

		// Check write permissions by attempting to create/open the file
		testFile, err := os.OpenFile(statsPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return nil, fmt.Errorf("cannot create or write to stats file %s: %w", statsPath, err)
		}
		testFile.Close()
	}

	inst := &Meter{
		cacheReader:   cache,
		incoming:      incData,
		statsFilePath: statsPath,
		MaxFileSize:   50 * 1024 * 1024,
		ctx:           ctx,
		done:          make(chan struct{}),
	}
	return inst, nil
}
