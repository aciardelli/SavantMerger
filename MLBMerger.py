import requests
import os
import subprocess
from bs4 import BeautifulSoup
import sys
import concurrent.futures
import argparse
from typing import Optional

class VideoMetadata:
    def __init__(self, video_page_url: Optional[str]=None):
        # video information
        self.video_page_url = video_page_url
        self.mp4_video_url = None
        
        # event information
        self.description = None # shohei ohtani homers on a line drive to center field
        self.count = None # what number
        self.batter = None # batter name
        self.pitcher = None # pitcher name
        self.balls = None # balls in count
        self.strikes = None # strikes in count
        self.pitch_type = None # pitch type
        self.pitch_velo = None # pitch velo
        self.exit_velo = None # exit velo
        self.distance = None # hit distance
        self.num_parks = None # homer in x/30 parks
        self.matchup = None # team matchup
        self.date = None # date

        self.description_map = {
            'Batter:': self.batter,
            'Pitcher:': self.pitcher,
            'Count:': self.count,
            'Pitch Type:': self.pitch_type,
            'Velocity:': self.pitch_velo,
            'Exit Velocity:': self.exit_velo,
            'Hit Distance:': self.distance,
            'HR:': self.num_parks,
            'Matchup:': self.matchup,
            'Date:': self.date
        }

    def get_video_data(self, soup):
        data_list = soup.find('div', class_='mod')
        if data_list:
            data_list_items = data_list.find_all('li')
            for data_list_item in data_list_items:
                self.parse_data_list(data_list_item)

    def parse_data_list(self, data_list_item):
        description = data_list_item.find('strong').get_text(strip=True)
        full_text = data_list_item.get_text(strip=True)
        other_text = full_text.replace(description, '').strip()
        if description in self.description_map:
            self.description_map[description] = other_text

    def print_data_list(self):
        print(self.description_map)

class SearchSection:
    def __init__(self, player_id: Optional[str]=None, month: Optional[str]=None, year: Optional[str]=None, game_date: Optional[str]=None, game_pk: Optional[str]=None, pitch_type: Optional[str]=None, play_id: Optional[str]=None, group_by: Optional[str]=None):
        self.player_id = player_id
        self.month = month
        self.year = year
        self.game_date = game_date
        self.game_pk = game_pk
        self.pitch_type = pitch_type
        self.play_id = play_id
        self.group_by = group_by

    # compile url for
    def compile_url(self, url):
        video_details_url = url[:-8] + '&type=details'
        if self.group_by == 'name' or self.group_by == 'team' or self.group_by == 'venue':
            video_details_url += f'&player_id={self.player_id}'

        elif self.group_by == 'name-date' or self.group_by == 'team-date':
            video_details_url += f'&player_id={self.player_id}&ep_game_date={self.game_date}&ep_game_pk={self.game_pk}'

        elif self.group_by == 'name-month' or self.group_by == 'team-month':
            video_details_url += f'&player_id={self.player_id}&ep_game_month={self.month}'

        elif self.group_by == 'name-month-year' or self.group_by == 'team-month-year':
            video_details_url += f'&player_id={self.player_id}&ep_game_month={self.month}&ep_game_year={self.year}'

        elif self.group_by == 'name-year' or self.group_by == 'team-year':
            video_details_url += f'&player_id={self.player_id}&ep_game_year={self.year}'

        elif self.group_by == 'name-event' or self.group_by == 'team-event':
            video_details_url += f'&player_id={self.player_id}&play_guid={self.play_id}'

        elif self.group_by == 'pitch-type' or self.group_by == 'team-pitch-type':
            video_details_url += f'&player_id={self.player_id}&ep_pitch_type={self.pitch_type}'
        else:
            return None

        return video_details_url

class MLBMerger:
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

    # get url of each individual video page - mp4 needs to be grabbed from this url
    def get_video_page_urls(self):
        search_section_video_urls = []
        
        for search_section in self.search_section_list:
            compiled_url = search_section.compile_url(self.url)
            if compiled_url:
                search_section_video_urls.append(compiled_url)

        for search_section_video_url in search_section_video_urls:
            soup = self.load_page(search_section_video_url)
            links = soup.find_all('a', href=True)
            for link in links:
                href = link.get('href')

                # https://baseballsavant.mlb.com/sporty-videos?playId=ffad0706-ee0f-3a44-9c09-3a3d48b9a4e8
                video_url = 'https://baseballsavant.mlb.com' + href
                self.video_data_list.append(VideoMetadata(video_url))

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
            mp4_link = video_element.find('source').get('src')
            video_data.mp4_video_url = mp4_link

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

    mm = MLBMerger(url,title)
    mm.parse_savant_page()
    mm.get_mp4s()
    mm.download_videos()
    mm.merge_videos()
