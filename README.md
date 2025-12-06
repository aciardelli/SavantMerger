# Baseball Savant Video Merger

## Overview

Command line scraper and merger for Baseball Savant search urls. 

I am tired of clicking through Baseball Savant links and being redirected each time when I want to watch videos.
This script fixes that issue. Any Baseball Savant url will be parsed, scraped, and merged into a singular video.

## Dependencies

### System Requirements

- Python 3.8 or higher
- FFmpeg

### Python Packages

Using uv (recommended):
```bash
uv sync
```

Or using pip:

```bash
pip install .
```

Or install dependencies manually:

```bash
pip install beautifulsoup4 requests
```

## Usage

### Basic Usage
```bash
python SavantMerger.py -u "https://baseballsavant.mlb.com/statcast_search?..."
```

### Options

- -u, --url: Baseball Savant statcast_search URL (required)
- -o, --output: Output filename (default: merged.mp4)
- -v, --verbose: Enable verbose logging
