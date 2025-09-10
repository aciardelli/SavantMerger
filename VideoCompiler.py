import requests
import os
import subprocess
from bs4 import BeautifulSoup
import sys

class Section:
    def __init__(self, player_id: str=None, month: str=None, year: str=None, game_date: str=None, game_pk: str=None, pitch_type: str=None, play_id: str=None, group_by: str=None):
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

class VideoCompiler:
    def __init__(self, url: str, output_path: str=None):
        self.url = url
        self.output_path = output_path
        self.section_list = [] # all search sections loaded

    # helper function to load pages
    def load_page(self, url):
        response = requests.get(url)
        html_content = response.text
        soup = BeautifulSoup(html_content, 'html.parser')
        return soup

    # get all search sections on savant page
    def get_search_sections(self):
        print("Loading Savant Page...")
        soup = self.load_page(self.url)

        table_rows = soup.find_all('tr', class_='search_row default-table-row')
        for row in table_rows:
            player_id = row.get('data-player-id')
            month = row.get('data-month')
            year = row.get('data-year')
            game_date = row.get('data-game-date')
            game_pk = row.get('data-game-pk')
            pitch_type = row.get('data-pitch-type')
            play_id = row.get('data-play-id')
            group_by = row.get('data-group-by')
            section = Section(player_id, month, year, game_date, game_pk, pitch_type, play_id, group_by)

            self.section_list.append(section)

    # go to each informational video url
    def get_player_video_pages(self):
        self.get_search_sections()

        section_video_urls = []
        for section in self.section_list:
            compiled_url = section.compile_url(self.url)
            if compiled_url:
                section_video_urls.append(compiled_url)

        video_urls = []
        for section_video_url in section_video_urls:
            soup = self.load_page(section_video_url)
            links = soup.find_all('a', href=True)
            for link in links:
                href = link.get('href')

                # https://baseballsavant.mlb.com/sporty-videos?playId=ffad0706-ee0f-3a44-9c09-3a3d48b9a4e8
                video_url = 'https://baseballsavant.mlb.com' + href
                video_urls.append(video_url)

        return video_urls

    # download mp4 links
    def get_mp4s(self):
        video_pages = self.get_player_video_pages()
        mp4_links = []

        print(f"Loading {len(video_pages)} video pages...")
        for video_page in video_pages:
            soup = self.load_page(video_page)

            video = soup.find('video')
            mp4 = video.find('source').get('src')
            mp4_links.append(mp4)

        return mp4_links

    # download videos from mp4 links
    def download_video(self, url, filename):
        response = requests.get(url, stream=True)
        
        with open(filename, 'wb') as f:
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
                    f.flush()

    # merge downloaded videos
    def merge_videos(self):
        mp4_links = self.get_mp4s()
        temp_files = []

        try:
            for i,url in enumerate(mp4_links):
                mp4_str = f"temp_video_{i}.mp4"
                temp_files.append(mp4_str)
                print(f"Downloading video {i+1}...")
                self.download_video(url, mp4_str)

            filelist = 'filelist.txt'
            with open(filelist, 'w') as f:
                for temp in temp_files:
                    f.write(f'file {temp}\n')

            print("Merging videos...")

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
            for temp in temp_files:
                if os.path.exists(temp):
                    os.remove(temp)

            if os.path.exists('filelist.txt'):
                os.remove('filelist.txt')

def valid_url(url):
    if 'https://baseballsavant.mlb.com/statcast_search' in url:
        return True
    return False

if __name__ == "__main__":
    if len(sys.argv) > 3:
        print("Too many arguments")
        sys.exit()
    elif len(sys.argv) > 2:
        title = sys.argv[1]
        url = sys.argv[2]

        if '.mp4' not in title:
            title += '.mp4'
    elif len(sys.argv) > 1:
        title = "merged.mp4"
        url = sys.argv[1]
    else:
        print("No url to compile...")
        sys.exit()

    if not valid_url(url):
        print("The url you entered is not valid")
        sys.exit()

    vc = VideoCompiler(url,title)
    vc.merge_videos()
