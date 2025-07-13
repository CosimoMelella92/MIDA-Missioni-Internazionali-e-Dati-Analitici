import tweepy
import pandas as pd
from typing import Dict, Any, List
from .base_collector import BaseCollector
import logging
from datetime import datetime
import time

class SocialMediaCollector(BaseCollector):
    """Collector for social media data"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.platform = config.get('platform', 'twitter')
        self.api_keys = config.get('api_keys', {})
        self.search_terms = config.get('search_terms', [])
        self.max_results = config.get('max_results', 100)
        
    def collect(self) -> pd.DataFrame:
        """Collect data from social media platforms"""
        if self.platform == 'twitter':
            return self._collect_twitter()
        else:
            self.logger.error(f"Unsupported platform: {self.platform}")
            return pd.DataFrame()
    
    def validate(self, data: pd.DataFrame) -> bool:
        """Validate social media data"""
        if data.empty:
            return False
            
        required_columns = self.config.get('required_columns', [])
        return all(col in data.columns for col in required_columns)
    
    def _collect_twitter(self) -> pd.DataFrame:
        """Collect data from Twitter"""
        try:
            # Initialize Twitter API client
            client = tweepy.Client(
                bearer_token=self.api_keys.get('bearer_token'),
                consumer_key=self.api_keys.get('api_key'),
                consumer_secret=self.api_keys.get('api_secret'),
                access_token=self.api_keys.get('access_token'),
                access_token_secret=self.api_keys.get('access_token_secret')
            )
            
            all_tweets = []
            
            for term in self.search_terms:
                try:
                    # Search tweets
                    tweets = client.search_recent_tweets(
                        query=term,
                        max_results=self.max_results,
                        tweet_fields=['created_at', 'public_metrics', 'lang']
                    )
                    
                    if tweets.data:
                        for tweet in tweets.data:
                            tweet_data = {
                                'text': tweet.text,
                                'created_at': tweet.created_at,
                                'id': tweet.id,
                                'lang': tweet.lang,
                                'retweet_count': tweet.public_metrics['retweet_count'],
                                'reply_count': tweet.public_metrics['reply_count'],
                                'like_count': tweet.public_metrics['like_count'],
                                'quote_count': tweet.public_metrics['quote_count'],
                                'search_term': term
                            }
                            all_tweets.append(tweet_data)
                            
                except Exception as e:
                    self.logger.error(f"Error searching tweets for term {term}: {str(e)}")
                    
                # Add delay to avoid rate limits
                time.sleep(1)
                
            return pd.DataFrame(all_tweets)
            
        except Exception as e:
            self.logger.error(f"Error initializing Twitter client: {str(e)}")
            return pd.DataFrame()
    
    def process(self, data: pd.DataFrame) -> pd.DataFrame:
        """Process social media data"""
        # Add collection timestamp
        data['collection_date'] = datetime.now()
        
        # Clean text data
        if 'text' in data.columns:
            data['text'] = data['text'].str.strip()
            
        # Convert dates to datetime
        if 'created_at' in data.columns:
            data['created_at'] = pd.to_datetime(data['created_at'])
            
        return data 