import os
import subprocess
from bs4 import BeautifulSoup
from bs4.element import Tag, ResultSet
import sys
import argparse
from typing import Optional, List
from dataclasses import dataclass
import logging
import asyncio
import aiofiles
import aiohttp

# constants
BASE_URL = 'https://baseballsavant.mlb.com'
MAX_WORKERS = 4
DEFAULT_OUTPUT = 'merged.mp4'

# Metadata From Video Page
@dataclass
class VideoMetadata:
    video_page_url: str
    mp4_video_url: Optional[str] = None
    
    description: Optional[str] = None
    count: Optional[str] = None
    batter: Optional[str] = None
    pitcher: Optional[str] = None # pitcher name
    balls: Optional[str] = None # balls in count
    strikes: Optional[str] = None # strikes in count
    pitch_type: Optional[str] = None # pitch type
    pitch_velo: Optional[str] = None # pitch velo
    exit_velo: Optional[str] = None # exit velo
    distance: Optional[str] = None # hit distance
    num_parks: Optional[str] = None # homer in x/30 parks
    matchup: Optional[str] = None # team matchup
    date: Optional[str] = None # date

    # const
    DESCRIPTION_MAP = {
        'Batter:': 'batter',
        'Pitcher:': 'pitcher',
        'Count:': 'count',
        'Pitch Type:': 'pitch_type',
        'Velocity:': 'pitch_velo',
        'Exit Velocity:': 'exit_velo',
        'Hit Distance:': 'distance',
        'HR:': 'num_parks',
        'Matchup:': 'matchup',
        'Date:': 'date'
    }
        
    def get_video_data(self, soup: BeautifulSoup) -> None:
        data_list = soup.find('div', class_='mod')
        if data_list:
            data_list_items = data_list.find_all('li')
            for data_list_item in data_list_items:
                self.parse_data_list(data_list_item)

    def parse_data_list(self, data_list_item: Tag) -> None:
        strong_element = data_list_item.find('strong')
        if strong_element:
            description = strong_element.get_text(strip=True)
            full_text = data_list_item.get_text(strip=True)
            other_text = full_text.replace(description, '').strip()
            
            if description in self.DESCRIPTION_MAP:
                field_name = self.DESCRIPTION_MAP[description]
                setattr(self, field_name, other_text)

# Savant Search Section
@dataclass
class SearchSection:
    player_id: Optional[str] = None
    month: Optional[str] = None
    year: Optional[str] = None
    game_date: Optional[str] = None
    game_pk: Optional[str] = None
    pitch_type: Optional[str] = None
    play_id: Optional[str] = None
    group_by: Optional[str] = None

############ SCRAPER ############
class SavantScraper:
    def __init__(self, url: str):
        self.url = url
        self.search_section_list = [] # all search sections loaded
        self.video_data_list = [] # video metadata

    async def load_page(self, session: aiohttp.ClientSession, url: str) -> Optional[BeautifulSoup]:
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as response:
                response.raise_for_status()
                html_content = await response.text()
                soup = BeautifulSoup(html_content, 'html.parser')
                return soup
        except aiohttp.ClientError as e:
            logging.error(f"Failed to load page {url}: {e}")
            return None
        except Exception as e:
            logging.error(f"Unexpected error loading {url}: {e}")
            return None
                
    # parses all search section rows
    def parse_search_rows(self, rows: ResultSet) -> None:
        for row in rows:
            player_id = row.get('data-player-id')
            month = row.get('data-month')
            year = row.get('data-year')
            game_date = row.get('data-game-date')
            game_pk = row.get('data-game-pk')
            pitch_type = row.get('data-pitch-type')
            play_id = row.get('data-play-id')
            group_by = row.get('data-group-by')
            search_section = SearchSection(player_id, month, year, game_date, game_pk, pitch_type, play_id, group_by)
            self.search_section_list.append(search_section)

    # compile url
    def compile_url(self, url: str, savant_section: SearchSection) -> Optional[str]:
        video_details_url = url[:-8] + '&type=details'
        GROUP_BY_PARAMS = {
            ('name', 'team', 'venue'): f'&player_id={savant_section.player_id}',
            ('name-date', 'team-date'): f'&player_id={savant_section.player_id}&ep_game_date={savant_section.game_date}&ep_game_pk={savant_section.game_pk}',
            ('name-month', 'team-month'): f'&player_id={savant_section.player_id}&ep_game_month={savant_section.month}',
            ('name-year', 'team-year'): f'&player_id={savant_section.player_id}&ep_game_year={savant_section.year}',
            ('name-month-year', 'team-month-year'): f'&player_id={savant_section.player_id}&ep_game_month={savant_section.month}&ep_game_year={savant_section.year}',
            ('name-event', 'team-event'): f'&player_id={savant_section.player_id}&play_guid={savant_section.play_id}',
            ('pitch-type', 'team-pitch-type'): f'&player_id={savant_section.player_id}&ep_pitch_type={savant_section.pitch_type}'
        }

        for group_types, params in GROUP_BY_PARAMS.items():
            if savant_section.group_by in group_types:
                return video_details_url + params
        
        return None

    # get url of each individual video page - mp4 needs to be grabbed from this url
    async def get_video_page_urls(self, session: aiohttp.ClientSession) -> None:
        search_section_video_urls = []
        
        for search_section in self.search_section_list:
            compiled_url = self.compile_url(self.url, search_section)
            if compiled_url:
                search_section_video_urls.append(compiled_url)

        for search_section_video_url in search_section_video_urls:
            soup = await self.load_page(session, search_section_video_url)
            if soup == None:
                logging.warning(f"Skipping failed page: {search_section_video_url}")
                continue
            links = soup.find_all('a', href=True)
            for link in links:
                href = link.get('href')
                if href:
                    # https://baseballsavant.mlb.com/sporty-videos?playId=ffad0706-ee0f-3a44-9c09-3a3d48b9a4e8
                    video_url = BASE_URL + str(href)
                    self.video_data_list.append(VideoMetadata(video_page_url=video_url))

    # get all search sections on savant page and their individual video page urls
    async def parse_savant_page(self, session: aiohttp.ClientSession) -> None:
        logging.info("Loading BaseballSavant query...")
        soup = await self.load_page(session, self.url)
        if soup == None:
            raise RuntimeError(f"Failed to load main Baseball Savant page: {self.url}")

        table_rows = soup.find_all('tr', class_='search_row default-table-row')
        if not table_rows:
            logging.warning("No search result rows found")
            return
        
        self.parse_search_rows(table_rows)
        if not self.search_section_list:
            logging.warning("No valid search sections parsed")
            return

        logging.info(f"Parsed {len(self.search_section_list)} search sections")

        await self.get_video_page_urls(session)
        if not self.video_data_list:
            logging.warning(f"No video URLs found")
            return

        logging.info(f"Found {len(self.video_data_list)} video URLs")

    # asyncio to store multiple mp4 links
    async def get_mp4_links(self, session: aiohttp.ClientSession) -> None:
        async def fetch_mp4_link(session: aiohttp.ClientSession, video_data: VideoMetadata) -> bool:
            try:
                video_page = video_data.video_page_url
                soup = await self.load_page(session, video_page)
                if soup == None:
                    logging.warning(f"Failed to load video page: {video_page}")
                    return False
                
                # parse metadata
                video_data.get_video_data(soup)

                video_element = soup.find('video')
                if not video_element:
                    logging.warning(f"No video element found on page: {video_page}")
                    return False

                source_element = video_element.find('source')
                if not source_element:
                    logging.warning(f"No source element found on page: {video_page}")
                    return False

                mp4_link = source_element.get('src')
                if not mp4_link:
                    logging.warning(f"No mp4 link found on page: {video_page}")
                    return False

                video_data.mp4_video_url = str(mp4_link) if mp4_link else None
                return True
            except Exception as e:
                logging.error(f"Error processing video: {video_data.video_page_url}: {e}")
                return False

        logging.info(f"Loading {len(self.video_data_list)} video pages...")
        tasks = [fetch_mp4_link(session, video_data) for video_data in self.video_data_list]
        await asyncio.gather(*tasks)

        valid_videos = [v for v in self.video_data_list if v.mp4_video_url]
        failed_count = len(self.video_data_list) - len(valid_videos)

        if failed_count > 0:
            logging.warning(f"{failed_count} videos failed to get mp4 urls")

        self.video_data_list = valid_videos
        logging.info(f"{len(self.video_data_list)} videos ready for download")

        # makes videos chronological - TODO make a function for this
        self.video_data_list = self.video_data_list[::-1]

############ MERGER ############
class SavantMerger:
    def __init__(self, video_data_list: List[VideoMetadata], output_path: Optional[str]=None):
        self.output_path = output_path
        self.video_data_list = video_data_list
        self.temp_files = []

    # download videos from mp4 links
    async def download_videos(self, session: aiohttp.ClientSession) -> None:
        async def download_video(session: aiohttp.ClientSession, i: int, video_data: VideoMetadata) -> Optional[str]:
            temp_filename = f"temp_video_{i}.mp4"
            # logging.info("Downloading video:", i)
            try:
                async with session.get(video_data.mp4_video_url) as response: 
                    response.raise_for_status()
                    async with aiofiles.open(temp_filename, 'wb') as f:
                        async for chunk in response.content.iter_chunked(262144):
                            await f.write(chunk)
                            # f.flush()

                return temp_filename
            except Exception as e:
                logging.error(f"Failed to download {video_data.mp4_video_url}: {e}")
                return None

        logging.info(f"Downloading {len(self.video_data_list)} videos...")
        tasks = [download_video(session, i, video_data) for i, video_data in enumerate(self.video_data_list)]
        self.temp_files = await asyncio.gather(*tasks)

        self.temp_files = [f for f in self.temp_files if f is not None]

    # merge downloaded videos
    def merge_videos(self) -> None:
        logging.info("Merging videos...")
        try:
            filelist = 'filelist.txt'
            with open(filelist, 'w') as f:
                for temp_file in self.temp_files:
                    f.write(f'file {temp_file}\n')

            if self.output_path == None:
                self.output_path =  './merged.mp4'

            cmd = [
                'ffmpeg',
                '-f', 'concat',
                '-safe', '0',
                '-i', filelist,
                '-c', 'copy',
                self.output_path,
                '-y'
            ]

            subprocess.run(cmd, check=True)
            print(f"Merged video saved as: {self.output_path}")

        except Exception as e:
            logging.error(f"Something went wrong: {e}")
        finally:
            for temp in self.temp_files:
                if os.path.exists(temp):
                    os.remove(temp)

            if os.path.exists('filelist.txt'):
                os.remove('filelist.txt')

def check_url(url: str) -> bool:
    if url.startswith('https://baseballsavant.mlb.com/statcast_search'):
        return True
    return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-u", "--url", help="BaseballSavant query url", type=str)
    parser.add_argument("-o", "--output", help="Video output name", type=str)
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose logging")
    args = parser.parse_args()
    
    if args.verbose:
        logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    else:
        logging.basicConfig(level=logging.WARNING, format='%(levelname)s: %(message)s')

    url = args.url
    title = args.output

    if not url:
        print("No url to compile...")
        sys.exit()
    if not check_url(url):
        print("The url you entered is not valid")
        sys.exit()

    if title:
        if '.mp4' not in title:
            title += '.mp4'
        
    if not args.output:
        print("Default output name of merged.mp4")
        title = DEFAULT_OUTPUT

    async def main():
        async with aiohttp.ClientSession() as session:
            ss = SavantScraper(url)
            await ss.parse_savant_page(session)
            await ss.get_mp4_links(session)

            sm = SavantMerger(ss.video_data_list, title)
            await sm.download_videos(session)
            sm.merge_videos()

    asyncio.run(main())
