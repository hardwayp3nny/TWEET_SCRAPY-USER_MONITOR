import httpx
import os
import re
import json
import csv
import time
from datetime import datetime, timedelta

from user_info import User_info

##########配置区域##########
cookie = 'auth_token=9e0d199238861338a788fcbcc3a5029bc2c8e52c; ct0=f5c63c0c7b9e57248c88567890e366084ad8b9969eb05afe2b61c6565c320dbbc8b8ed9f9fe271530943bb25cf826829cab68b7e88fdb9dfbd643e6e04e786ea4764c4ec1001caf123a681b25d6fc00d;'
user_lst = ['elonmusk', 'OnchainDataNerd','layerggofficial']

from datetime import datetime, timedelta, timezone


def get_time_range():
    # 当前时间
    end_time = datetime.now()

    # 计算起始时间（30分钟前）
    start_time = end_time - timedelta(minutes=30)

    # 转换为UTC时间
    end_time_utc = end_time.astimezone(timezone.utc)
    start_time_utc = start_time.astimezone(timezone.utc)

    # 格式化时间
    return f"{start_time_utc.strftime('%Y-%m-%d %H:%M:%S')} - {end_time_utc.strftime('%Y-%m-%d %H:%M:%S')}"


time_range = get_time_range()



##########配置区域##########

def time2stamp(timestr: str) -> int:
    datetime_obj = datetime.strptime(timestr, "%Y-%m-%d %H:%M:%S")
    msecs_stamp = int(time.mktime(datetime_obj.timetuple()) * 1000.0 + datetime_obj.microsecond / 1000.0)
    return msecs_stamp

start_time_str, end_time_str = time_range.split(' - ')
start_time_stamp = time2stamp(start_time_str)
end_time_stamp = time2stamp(end_time_str)

class csv_gen():
    def __init__(self, save_path: str, user_name, screen_name, tweet_range) -> None:
        self.f = open(f'{save_path}/{screen_name}-{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}-text.csv', 'w',
                      encoding='utf-8-sig', newline='')
        self.writer = csv.writer(self.f)

        # 初始化
        self.writer.writerow([user_name, '@' + screen_name])
        self.writer.writerow(['Tweet Range : ' + tweet_range])
        self.writer.writerow(['Save Path : ' + save_path])
        main_par = ['Tweet Date', 'Tweet URL', 'Tweet Content', 'Favorite Count',
                    'Retweet Count', 'Reply Count']
        self.writer.writerow(main_par)

    def csv_close(self):
        self.f.close()

    def stamp2time(self, msecs_stamp: int) -> str:
        timeArray = time.localtime(msecs_stamp / 1000)
        otherStyleTime = time.strftime("%Y-%m-%d %H:%M", timeArray)
        return otherStyleTime

    def data_input(self, main_par_info: list) -> None:  # 数据格式参见 main_par
        main_par_info[0] = self.stamp2time(main_par_info[0])  # 传进来的是 int 时间戳, 故转换一下
        self.writer.writerow(main_par_info)

def time_comparison(now):
    start_label = True
    start_down = False
    # twitter : latest -> old
    if now >= start_time_stamp and now <= end_time_stamp:  # 符合时间条件，下载
        start_down = True
    elif now < start_time_stamp:  # 超出时间范围，结束
        start_label = False
    return [start_down, start_label]

def get_other_info(_user_info, _headers):
    url = 'https://twitter.com/i/api/graphql/xc8f1g7BYqr6VTzTbvNlGw/UserByScreenName?variables={"screen_name":"' + _user_info.screen_name + '","withSafetyModeUserFields":false}&features={"hidden_profile_likes_enabled":false,"hidden_profile_subscriptions_enabled":false,"responsive_web_graphql_exclude_directive_enabled":true,"verified_phone_label_enabled":false,"subscriptions_verification_info_verified_since_enabled":true,"highlights_tweets_tab_ui_enabled":true,"creator_subscriptions_tweet_preview_api_enabled":true,"responsive_web_graphql_skip_user_profile_image_extensions_enabled":false,"responsive_web_graphql_timeline_navigation_enabled":true}&fieldToggles={"withAuxiliaryUserLabels":false}'
    try:
        response = httpx.get(url, headers=_headers).text
        raw_data = json.loads(response)
        _user_info.rest_id = raw_data['data']['user']['result']['rest_id']
        _user_info.name = raw_data['data']['user']['result']['legacy']['name']
        _user_info.statuses_count = raw_data['data']['user']['result']['legacy']['statuses_count']
        _user_info.media_count = raw_data['data']['user']['result']['legacy']['media_count']
    except Exception:
        print('获取信息失败')
        print(response)
        return False
    return True

def print_info(_user_info):
    print(
        f'''
        <======基本信息=====>
        昵称:{_user_info.name}
        用户名:{_user_info.screen_name}
        数字ID:{_user_info.rest_id}
        总推数(含转推):{_user_info.statuses_count}
        含图片/视频/音频推数(不含转推):{_user_info.media_count}
        <==================>
        开始爬取...
        '''
    )

class text_down():
    def __init__(self, screen_name):
        self._user_info = User_info(screen_name)

        self._headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
            'authorization': 'Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA',
        }
        self._headers['cookie'] = cookie
        re_token = 'ct0=(.*?);'
        self._headers['x-csrf-token'] = re.findall(re_token, cookie)[0]

        if not get_other_info(self._user_info, self._headers):
            return
        print_info(self._user_info)

        self._headers['referer'] = 'https://twitter.com/' + self._user_info.screen_name

        self.folder_path = os.getcwd() + os.sep + screen_name + os.sep

        if not os.path.exists(self.folder_path):  # 创建文件夹
            os.makedirs(self.folder_path)

        self.csv_file = csv_gen(self.folder_path, self._user_info.name, self._user_info.screen_name, time_range)

        self.cursor = ''

        self.get_clean_save()

        self.csv_file.csv_close()

    def get_clean_save(self):
        while True:
            url = 'https://twitter.com/i/api/graphql/9zyyd1hebl7oNWIPdA8HRw/UserTweets?variables={"userId":"' + self._user_info.rest_id + '","count":20,"cursor":"' + self.cursor + '","includePromotedContent":true,"withQuickPromoteEligibilityTweetFields":true,"withVoice":true,"withV2Timeline":true}&features={"rweb_tipjar_consumption_enabled":true,"responsive_web_graphql_exclude_directive_enabled":true,"verified_phone_label_enabled":false,"creator_subscriptions_tweet_preview_api_enabled":true,"responsive_web_graphql_timeline_navigation_enabled":true,"responsive_web_graphql_skip_user_profile_image_extensions_enabled":false,"communities_web_enable_tweet_community_results_fetch":true,"c9s_tweet_anatomy_moderator_badge_enabled":true,"articles_preview_enabled":true,"tweetypie_unmention_optimization_enabled":true,"responsive_web_edit_tweet_api_enabled":true,"graphql_is_translatable_rweb_tweet_is_translatable_enabled":true,"view_counts_everywhere_api_enabled":true,"longform_notetweets_consumption_enabled":true,"responsive_web_twitter_article_tweet_consumption_enabled":true,"tweet_awards_web_tipping_enabled":false,"creator_subscriptions_quote_tweet_preview_enabled":false,"freedom_of_speech_not_reach_fetch_enabled":true,"standardized_nudges_misinfo":true,"tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled":true,"tweet_with_visibility_results_prefer_gql_media_interstitial_enabled":true,"rweb_video_timestamps_enabled":true,"longform_notetweets_rich_text_read_enabled":true,"longform_notetweets_inline_media_enabled":true,"responsive_web_enhance_cards_enabled":false}&fieldToggles={"withArticlePlainText":false}'

            response = httpx.get(url, headers=self._headers).text
            raw_data = json.loads(response)
            raw_tweet_lst = raw_data['data']['user']['result']['timeline_v2']['timeline']['instructions'][-1]['entries']

            if len(raw_tweet_lst) == 2:
                return
            if self.cursor == raw_tweet_lst[-1]['content']['value']:
                return
            self.cursor = raw_tweet_lst[-1]['content']['value']

            for tweet in raw_tweet_lst:
                if 'promoted-tweet' in tweet['entryId']:  # 排除广告
                    continue
                if 'tweet' in tweet['entryId']:
                    tweet_data = tweet['content']['itemContent']['tweet_results']['result']
                    if 'legacy' not in tweet_data:
                        continue  # Skip this tweet if it doesn't have the expected structure
                    raw_text = tweet_data['legacy']

                    # Extract and convert the timestamp
                    created_at_str = raw_text['created_at']
                    created_at_dt = datetime.strptime(created_at_str, "%a %b %d %H:%M:%S +0000 %Y")
                    _time_stamp = int(created_at_dt.timestamp() * 1000)  # Convert to milliseconds

                    _results = time_comparison(_time_stamp)
                    if not _results[1]:  # 超出时间范围，结束
                        return
                    if not _results[0]:  # 不符合时间条件，跳过
                        continue

                    _Favorite_Count = raw_text['favorite_count']
                    _Retweet_Count = raw_text['retweet_count']
                    _Reply_Count = raw_text['reply_count']
                    _status_id = raw_text['id_str']
                    _tweet_url = f'https://twitter.com/{self._user_info.screen_name}/status/{_status_id}'
                    _tweet_content = raw_text['full_text'].split('https://t.co/')[0]

                    self.csv_file.data_input(
                        [_time_stamp, _tweet_url, _tweet_content, _Favorite_Count, _Retweet_Count, _Reply_Count])
if __name__ == '__main__':
    for user in user_lst:
        text_down(user)
    print('完成 (๑´ڡ`๑)')