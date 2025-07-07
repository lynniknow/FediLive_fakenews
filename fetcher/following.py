# fetcher/reblog_favourite.py
import random
import requests
import time
import re
import argparse
from multiprocessing import Process
from .utils import judge_api_islimit, judge_sleep_limit_table
from .database_manager import DatabaseManager
from .livefeeds_manager import LivefeedsManager
from .config import Config
import logging 

logger = logging.getLogger(__name__)
config = Config()

limit_dict = {}#[{} for i in token_num]
limit_set = set()#[set() for i in token_num]

def get_following(db_manager, token, instance, user_id, retry_thresh=4):
    url = f"https://{instance}/api/v1/accounts/{user_id}/following"
    HEADERS = {
        'Authorization': f'Bearer {token}',
        'Email': config.api.get('email', '')
    }
    followings = []
    # pids = set()
    
    last_page_flag = -1
    retry_time = 0
    
    while True: #fedi.sphericalcow.space#ASNk3RPuZ0H3eTdCb2
        params = {'limit': 40}
        if last_page_flag != -1:
            params['max_id'] = last_page_flag
        # print(f"yesyes{len(followings)}")
        try:
            response = requests.get(url, headers=HEADERS, params=params, timeout=5)
            if response.status_code == 200:
                res_headers = {k.lower(): v for k, v in response.headers.items()}
                judge_sleep_limit_table(res_headers,instance,limit_dict,limit_set)
                # temp = set(followings) & set(response.json())
                # print(f"nono{len(temp)}")
                data = response.json()
                followings.extend(data)
                
                # cur = set()
                # for user in data:
                #     u = user.get('url').split('/')[2] + '#' + user.get('username')
                #     cur.add(u)
                #     pids.add(u)
                # print(f'nono{len(pids)}')
                # print(response.json())

                if 'Link' not in response.headers or len(response.json()) < 40:
                    break
                
                if len(followings) >= 2000:
                    livefeeds_manager = LivefeedsManager(db_manager.connection)
                    livefeeds_manager.save_luser(instance, user_id)
                    return True

                match = re.search(r'max_id=(\d+)', response.headers['Link'])
                if match:
                    last_page_flag = match.group(1)

            elif response.status_code in (429, 503):
                retry_time += 1
                time.sleep(random.random())
                logger.warning("Encountered 429 or 503 error, retrying...")
                if retry_time <= retry_thresh:
                    continue
                else:
                    retry_time = 0
                    db_manager.insert_error_log("following", f"{instance}#{user_id}", "429or503")
                    return False

            else:
                db_manager.insert_error_log("following", f"{instance}#{user_id}", "HTTPError", response_code=response.status_code)
                logger.error(f"Error fetching following for {instance}#{user_id}: {response.status_code}")
                return False

        except requests.exceptions.Timeout:
            retry_time += 1
            time.sleep(0.1)
            logger.warning("Request timed out, retrying...")
            if retry_time <= retry_thresh:
                continue
            else:
                retry_time = 0
                db_manager.insert_error_log("following", f"{instance}#{user_id}", "Timeout")
                return False

        except Exception as e:
            db_manager.insert_error_log("following", f"{instance}#{user_id}", "Exception", error_message=str(e))
            logger.exception(f"Exception while connecting to {instance}#{user_id}: {e}")
            return False
    livefeeds_manager = LivefeedsManager(db_manager.connection)
    livefeeds_manager.save_followings(instance, user_id, followings)
    return True


def fetch_user_id(db_manager, retry_thresh=10):
    """
    Fetches the next status ID to process from the `livefeeds` table in the database.

    Args:
        db_manager: A database manager object used to execute queries and manage connections.
        retry_thresh (int, optional): The maximum number of retry attempts before resetting the retry counter.

    Returns:
        tuple: 
            - (tuple): A tuple containing the next candidate's details.
            - (bool): `False` if a candidate is successfully fetched, `True` if the processing should terminate.
    """
    retry_time = 0

    while True:
        instance_query = f"'{','.join(limit_set)}'" if limit_set else ""
        if instance_query:
            query = f"""
                UPDATE users
                SET status = 'read'
                WHERE pid = (
                    SELECT pid
                    FROM users
                    WHERE status = 'pending'
                    AND (instance_name NOT IN ({instance_query}))
                    LIMIT 1
                )
                RETURNING pid, status, instance_name, id
            """
        else:
            query = """
                UPDATE users
                SET status = 'read'
                WHERE pid = (
                    SELECT pid
                    FROM users
                    WHERE status = 'pending'
                    LIMIT 1
                )
                RETURNING pid, status, instance_name, id
            """
        cursor = db_manager.connection.execute(query)
        candidate = cursor.fetchone()

        if not candidate and not limit_set:
            logger.info("No eligible user found and limit_set is empty. Terminating task.")
            return None, True

        if candidate:
            logger.info(f"Found user ID: {candidate[0]}")
            return candidate, False

        logger.info(f"No matching users found, retrying... Attempt {retry_time}")
        time.sleep(2)
        retry_time += 1

        if retry_time >= retry_thresh:
            judge_api_islimit(limit_dict, limit_set)
            retry_time = 0

def process_task(token):
    """
    Main processing loop to fetch and update livefeeds data using the provided API token.

    Args:
        token (str): Authorization token used for API requests.
    """
    # db_manager = DatabaseManager()
    # livefeeds_manager = LivefeedsManager(db_manager.connection)
    current_processing_status = None
    terminate_process = False
    while not terminate_process:
        db_manager = DatabaseManager()
        try:
            judge_api_islimit(limit_dict, limit_set)
            info, terminate_process = fetch_user_id(db_manager)
            if info:
                current_processing_status = info
                get_following(db_manager, token, current_processing_status[2], current_processing_status[3])
                logger.info(f"Successfully fetched followings for {info[0]}")
                current_processing_status = None
            else:
                if terminate_process:
                    return
                logger.info("No pending users found, sleeping...")
                time.sleep(60)
        except Exception as e:
            logger.exception(f"Exception during processing: {e}")
            if current_processing_status:
                update_query = """
                UPDATE users
                    SET status = 'pending'
                    WHERE pid = ?
                """
                db_manager.connection.execute(update_query, (current_processing_status[0],))
                current_processing_status = None
            time.sleep(5)
        db_manager.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch followings and save to JSON.")
    parser.add_argument('--processnum', type=int, default=1, help='processing num')
    parser.add_argument("--output", type=str, default="following.json", help="Output file for livefeeds data.")
    args = parser.parse_args()
    db_manager = DatabaseManager()
    db_manager.create_tables()
    db_manager.check_status()
    db_manager.close()
    token = config.api.get('livefeeds_token')
    
    process_list = []
    for i in range(args.processnum):
        p = Process(target=process_task, args=(token,))
        p.start()
        process_list.append(p)
    for item in process_list:
        item.join()
    db_manager = DatabaseManager()
    livefeeds_manager = LivefeedsManager(db_manager.connection)
    livefeeds_manager.export_to_json(args.output, table="followings")
    db_manager.close()

    logger.info("All tasks completed successfully.")