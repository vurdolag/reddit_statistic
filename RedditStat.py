
from asyncpraw import Reddit
from Server import Server
from asyncio import sleep
from util import BD, loop, CommentBD, PostBD, re_q2, re_q
from time import time
from typing import List
import config


class RedditStat:
    def __init__(self, sub):
        self.target_sub = sub
        self.reddit_session = None
        self.max_get_post = 1000
        self.max_time_stat = 3600*24*2

        self.parser = (
            (3600, 60*15),
            (3600*3, 60*30),
            (3600*6, 60*45),
            (3600*24, 60*90),
            (3600*24*3, 60*60*4),
            (3600*24*7, 60*60*8),
            (3600*24*14, 60*60*16),
            (3600*24*30, 60*60*30),
            (3600*24*90, 60*60*24*5),
        )

        self.parser = sorted(self.parser, reverse=True)

    def auth(self, client_id: str, client_secret: str):
        user_agent = ('Mozilla/5.0 (Windows NT 6.2; WOW64; rv:53.0) AppleWebKit/534.50.2 '
                      'Firefox/49.0 Chrome/58.0.2902.81 Chromium/49.0.2623.108 OPR/43.0.2442.849')

        self.reddit_session = Reddit(client_id=client_id,
                                     client_secret=client_secret,
                                     user_agent=user_agent)

    async def get_new_posts(self):
        while True:
            subreddit = await self.reddit_session.subreddit(self.target_sub)
            t = int(time())
            ind = 0
            async for post in subreddit.new(limit=self.max_get_post):
                ind += 1
                _time = int(post.created_utc)
                if t - _time > self.max_time_stat:
                    break

                self.save_post(post, t)

                if ind % 25 == 0:
                    print(ind, '/', self.max_get_post)

            await sleep(60*20)

    def save_post(self, post, t):
        ans: List[PostBD] = BD.post.get(post.id)
        if not ans:
            content = str(post.media or post.selftext or post.url)
            score = f'{post.score}={t}'
            BD.post.put(post.id, post.title, post.score, score, content, int(post.created_utc), t)
        else:
            score = f'{ans[0].score},{post.score}={t}'
            ans[0].up(score_now=post.score, score=score)

    async def get_comments(self, post_id: str):
        post = await self.reddit_session.submission(post_id)
        comments_list = await post.comments()
        await comments_list.replace_more(limit=None)
        now_time = time()
        for j, comment in enumerate(await comments_list.list()):
            if now_time - post.created_utc > self.max_time_stat:
                continue

            if comment:
                self.save_comment(self.comment(comment, post_id))
            else:
                print(comment)

            #if j % 25 == 0:
            #    print('get comment', post_id, j)

    def comment(self, comment, post_id):
        root_id = '0' if comment.is_root else comment.parent_id.split('_')[-1]
        author = comment.author.name if comment.author else '[delete]'
        return CommentBD(comment.id, root_id, post_id, comment.body, comment.score,
                         comment.score,  int(comment.created_utc), author)

    def save_comment(self, comment: CommentBD):
        ans: List[CommentBD] = BD.comment.get(comment.id)
        t = int(time())
        body = re_q.sub("'", comment.body)
        body = re_q2.sub(',', body)

        if ans:
            ans_last_body = ans[0].body.split(',,')[-1].split('||')[0]
        else:
            ans_last_body = ''

        if not ans:
            body = f'{body}||{t}'
            score = f'{comment.score}={t}'
            BD.comment.put(comment.id, comment.root_id, comment.post_id, body,
                           comment.score, score, comment.time, comment.author)

        elif ans_last_body != body:
            body = f'{ans[0].body},,{body}||{t}'
            score = f'{ans[0].score},{comment.score}={t}'
            BD.comment.up(comment.id, body=body, score=score, score_now=comment.score)

        else:
            score = f'{ans[0].score},{comment.score}={t}'
            BD.comment.up(comment.id, score=score, score_now=comment.score)

    def is_post_ok(self, post: PostBD):
        now_time = int(time())

        last_t = int(now_time - post.time_pars)
        t = int(now_time - post.time_post)

        for p, p_itr in self.parser:
            if p < t:
                if last_t > p_itr:
                    post.up(time_pars=now_time)
                    return True
                else:
                    return False

        return False

    async def get_new_comments(self):
        ans: List[PostBD] = BD.post.get_all()
        count = 1
        for i, post in enumerate(ans):

            if not self.is_post_ok(post):
                continue

            await self.get_comments(post.id)
            await sleep(0.1)

            print('get comment post', post.id, count, i, '/', len(ans))

            count += 1

        print('end', count - 1, len(ans))

    async def main(self):
        while True:
            await self.get_new_comments()
            await sleep(60*6)


async def on_start(_):
    stat = RedditStat(config.sub)
    stat.auth(config.client_id, config.client_secret)

    loop.add(stat.get_new_posts()).add(stat.main()).start()


if __name__ == '__main__':
    Server().add(on_start).run()
