import requests
import os
import subprocess
from bs4 import BeautifulSoup
import sys
import concurrent.futures
import argparse
from typing import Optional
from dataclasses import dataclass

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

    def get_video_data(self, soup):
        data_list = soup.find('div', class_='mod')
        if data_list:
            data_list_items = data_list.find_all('li')
            for data_list_item in data_list_items:
                self.parse_data_list(data_list_item)

    def parse_data_list(self, data_list_item):
        description_map = {
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
        
        strong_element = data_list_item.find('strong')
        if strong_element:
            description = strong_element.get_text(strip=True)
            full_text = data_list_item.get_text(strip=True)
            other_text = full_text.replace(description, '').strip()
            
            if description in description_map:
                field_name = description_map[description]
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

############ MERGER ############
class SavantMerger:
    def __init__(self, url: str, output_path: Optional[str]=None):
        self.url = url
        self.output_path = output_path
        self.search_section_list = [] # all search sections loaded
        self.video_data_list = [] # video metadata
        self.temp_files = []

    # helper function to load page html
    def load_page(self, url):
        response = requests.get(url)
        html_content = response.text
        soup = BeautifulSoup(html_content, 'html.parser')
        return soup

    # parses all search section rows
    def parse_search_rows(self, rows):
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
    def compile_url(self, url: str, savant_section: SearchSection):
        video_details_url = url[:-8] + '&type=details'
        if savant_section.group_by == 'name' or savant_section.group_by == 'team' or savant_section.group_by == 'venue':
            video_details_url += f'&player_id={savant_section.player_id}'

        elif savant_section.group_by == 'name-date' or savant_section.group_by == 'team-date':
            video_details_url += f'&player_id={savant_section.player_id}&ep_game_date={savant_section.game_date}&ep_game_pk={savant_section.game_pk}'

        elif savant_section.group_by == 'name-month' or savant_section.group_by == 'team-month':
            video_details_url += f'&player_id={savant_section.player_id}&ep_game_month={savant_section.month}'

        elif savant_section.group_by == 'name-month-year' or savant_section.group_by == 'team-month-year':
            video_details_url += f'&player_id={savant_section.player_id}&ep_game_month={savant_section.month}&ep_game_year={savant_section.year}'

        elif savant_section.group_by == 'name-year' or savant_section.group_by == 'team-year':
            video_details_url += f'&player_id={savant_section.player_id}&ep_game_year={savant_section.year}'

        elif savant_section.group_by == 'name-event' or savant_section.group_by == 'team-event':
            video_details_url += f'&player_id={savant_section.player_id}&play_guid={savant_section.play_id}'

        elif savant_section.group_by == 'pitch-type' or savant_section.group_by == 'team-pitch-type':
            video_details_url += f'&player_id={savant_section.player_id}&ep_pitch_type={savant_section.pitch_type}'
        else:
            return None

        return video_details_url

    # get url of each individual video page - mp4 needs to be grabbed from this url
    def get_video_page_urls(self):
        search_section_video_urls = []
        
        for search_section in self.search_section_list:
            compiled_url = self.compile_url(self.url, search_section)
            if compiled_url:
                search_section_video_urls.append(compiled_url)

        for search_section_video_url in search_section_video_urls:
            soup = self.load_page(search_section_video_url)
            links = soup.find_all('a', href=True)
            for link in links:
                href = link.get('href')
                if href:
                    # https://baseballsavant.mlb.com/sporty-videos?playId=ffad0706-ee0f-3a44-9c09-3a3d48b9a4e8
                    video_url = 'https://baseballsavant.mlb.com' + str(href)
                    self.video_data_list.append(VideoMetadata(video_page_url=video_url))

    # get all search sections on savant page and their individual video page urls
    def parse_savant_page(self):
        print("Loading BaseballSavant query...")
        soup = self.load_page(self.url)
        table_rows = soup.find_all('tr', class_='search_row default-table-row')
        self.parse_search_rows(table_rows)

        self.get_video_page_urls()

    # multithreading to store multiple mp4 links
    def get_mp4s(self):
        def get_mp4_link(video_data):
            video_page = video_data.video_page_url
            soup = self.load_page(video_page)
            
            # parse metadata
            video_data.get_video_data(soup)

            video_element = soup.find('video')
            if video_element:
                source_element = video_element.find('source')
                if source_element:
                    mp4_link = source_element.get('src')
                    video_data.mp4_video_url = str(mp4_link) if mp4_link else None

        print(f"Loading {len(self.video_data_list)} video pages...")
        with concurrent.futures.ThreadPoolExecutor(max_workers = 4) as executor:
            executor.map(get_mp4_link, self.video_data_list)

    # download videos from mp4 links
    def download_videos(self):
        def download_video(args):
            i, video_data = args
            temp_filename = f"temp_video_{i}.mp4"

            response = requests.get(video_data.mp4_video_url, stream=True)
            
            with open(temp_filename, 'wb') as f:
                for chunk in response.iter_content(chunk_size=1024):
                    if chunk:
                        f.write(chunk)
                        f.flush()

            return temp_filename

        print(f"Downloading {len(self.video_data_list)} videos...")
        with concurrent.futures.ThreadPoolExecutor(max_workers = 4) as executor:
            tasks = [(i, video_data) for i, video_data in enumerate(self.video_data_list)]
            self.temp_files = list(executor.map(download_video, tasks))

    # merge downloaded videos
    def merge_videos(self):
        print("Merging videos...")
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
            print(f"Something went wrong: {e}")
        finally:
            for temp in self.temp_files:
                if os.path.exists(temp):
                    os.remove(temp)

            if os.path.exists('filelist.txt'):
                os.remove('filelist.txt')

def valid_url(url):
    if 'https://baseballsavant.mlb.com/statcast_search' in url:
        return True
    return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-u", "--url", help="BaseballSavant query url", type=str)
    parser.add_argument("-o", "--output", help="Video output name", type=str)
    args = parser.parse_args()

    url = args.url
    title = args.output

    if not url:
        print("No url to compile...")
        sys.exit()
    if not valid_url(url):
        print("The url you entered is not valid")
        sys.exit()

    if title:
        if '.mp4' not in title:
            title += '.mp4'
        
    if not args.output:
        print("Default output name of merged.mp4")
        title = "merged.mp4"

    sm = SavantMerger(url,title)
    sm.parse_savant_page()
    sm.get_mp4s()
    sm.download_videos()
    sm.merge_videos()
