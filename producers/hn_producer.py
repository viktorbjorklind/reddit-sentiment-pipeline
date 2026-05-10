import os
import json
import time
import logging
import requests
from kafka import KafkaProducer
from dotenv import load_dotenv

# Load credentials from .env file
load_dotenv()

# Set up logging so we can see what's happening when the script runs
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s' 
)
logger = logging.getLogger(__name__)

# Read config from environment variables (never hardcoded!)
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC_POSTS', 'hn-posts-raw')

# Hacker News API base url - no auth needed
HN_BASE_URL = 'https://hacker-news.firebaseio.com/v0'


def get_top_story_ids() -> list[int]:
    '''Fetch the current list of top 500 story IDs from Hacker News'''
    response = requests.get(f'{HN_BASE_URL}/topstories.json', timeout=10)
    response.raise_for_status()
    return response.json()


def get_story(story_id: int) -> dict | None:
    '''Fetch the full details of a single story by its ID'''
    response = requests.get(f'{HN_BASE_URL}/item/{story_id}.json', timeout=10)
    response.raise_for_status()
    return response.json()


def create_producer() -> KafkaProducer:
    '''
    Create and return a Kafka producer.
    value_serializer converts our Python dict to JSON bytes automatically every time we call producer.send()
    '''
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        retries=3
    )


def run():
    '''
    Main loop: fetch top HN stories and publish each one to Kafka.
    Tracks seen IDs so we don't publish the same story twice.
    Polls every 60 seconds.
    '''
    logger.info('Starting Hacker News producer...')
    producer = create_producer()
    seen_ids = set()

    while True:
        try:
            top_ids = get_top_story_ids()
            # Only process stories we haven't seen yet
            new_ids = [id for id in top_ids[:50] if id not in seen_ids]
            logger.info(f'Found {len(new_ids)} new stories to publish.')

            for story_id in new_ids:
                story = get_story(story_id)

                # Skip non-stories (e.g. job posts, polls) and stories with no title
                if not story or story.get('type') != 'story' or not story.get('title'):
                    continue

                # Send to Kafka - producer serialises this dict to JSON automatically
                producer.send(KAFKA_TOPIC, value=story)
                seen_ids.add(story_id)
                logger.info(f'Published story {story_id}: {story.get('title', '')[:60]}')

                # Small delay to be respectful to the HN API rate limits
                time.sleep(0.1)

            producer.flush()
            logger.info(f'Sleeping 60 seconds before next poll...')
            time.sleep(60)

        except requests.RequestException as e:
            logger.error(f'HTTP error fetching from HN API: {e}')
            time.sleep(30)

        except Exception as e:
            logger.error(f'Unexpected error: {e}')
            time.sleep(30)

if __name__ == '__main__':
    run()