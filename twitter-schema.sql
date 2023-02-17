SET TIME ZONE 'GMT' ;
CREATE TABLE authority (
       id text NOT NULL ,
       obj_type text NOT NULL ,
       orig jsonb NOT NULL ,
       meta_inserted_at timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP ,
       meta_updated_at timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP ,
       meta_source text NOT NULL ,
       auth_source text ,
       auth_date timestamp with time zone ,
       CONSTRAINT authority_meta_source_id_key UNIQUE ( meta_source , id )
) ;
CREATE TABLE tweets (
       /* ["rest_id"] */
       /* == ["legacy"]["id_str"] */
       id text PRIMARY KEY ,
       /* ["legacy"]["created_at"] */
       created_at timestamp with time zone NOT NULL ,
       /* ["legacy"]["user_id_str"] */
       /* == ["core"]["user_results"]["result"]["rest_id"] */
       author_id text NOT NULL ,
       /* ["core"]["user_results"]["result"]["legacy"]["screen_name"] */
       author_screen_name text ,
       /* ["legacy"]["conversation_id_str"] */
       conversation_id text ,
       /* ["legacy"]["self_thread"]["id_str"] */
       thread_id text ,
       /* ["legacy"]["in_reply_to_status_id_str"] */
       replyto_id text ,
       /* ["legacy"]["in_reply_to_user_id_str"] */
       replyto_author_id text ,
       /* ["legacy"]["in_reply_to_screen_name"] */
       replyto_author_screen_name text ,
       /* ["legacy"]["retweeted_status_result"]["result"]["rest_id"] */
       retweeted_id text ,
       /* ["legacy"]["retweeted_status_result"]["result"]["legacy"]["user_id_str"] */
       /* == ["legacy"]["retweeted_status_result"]["result"]["core"]["user_results"]["result"]["rest_id"] */
       retweeted_author_id text ,
       /* ["legacy"]["retweeted_status_result"]["result"]["core"]["user_results"]["result"]["legacy"]["screen_name"] */
       retweeted_author_screen_name text ,
       /* ["legacy"]["quoted_status_id_str"] */
       /* == ["quoted_status_result"]["result"]["rest_id"] */
       quoted_id text ,
       /* ["quoted_status_result"]["result"]["legacy"]["user_id_str"] */
       /* == ["quoted_status_result"]["result"]["core"]["user_results"]["result"]["rest_id"] */
       quoted_author_id text ,
       /* ["quoted_status_result"]["result"]["core"]["user_results"]["result"]["legacy"]["screen_name"] */
       quoted_author_screen_name text ,
       /* ["legacy"]["full_text"], as provided in JSON (normally HTML-quoted) */
       full_text text NOT NULL ,
       /* reconstructed from full_text, URLs substituted by expanded_url, HTML-unquoted */
       input_text text NOT NULL ,
       /* ["legacy"]["lang"] */
       lang text ,
       /* ["legacy"]["favorite_count"] */
       favorite_count int ,
       /* ["legacy"]["retweet_count"] */
       retweet_count int ,
       /* ["legacy"]["quote_count"] */
       quote_count int ,
       /* ["legacy"]["reply_count"] */
       reply_count int ,
       meta_inserted_at timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP ,
       meta_updated_at timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP ,
       meta_source text NOT NULL
) ;
CREATE TABLE media (
       /* ["id_str"] */
       id text PRIMARY KEY ,
       /* id of parent tweet, not always avail as ["source_status_id_str"] */
       parent_id text NOT NULL ,
       /* ["url"] */
       short_url text NOT NULL ,
       /* ["display_url"] */
       display_url text NOT NULL ,
       /* ["media_url_https"] */
       media_url text ,
       /* ["type"] */
       media_type text NOT NULL ,
       /* ["ext_alt_text"] */
       alt_text text ,
       meta_inserted_at timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP ,
       meta_updated_at timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP ,
       meta_source text NOT NULL
) ;
CREATE TABLE users (
       /* ["rest_id"] */
       id text PRIMARY KEY ,
       /* ["legacy"]["created_at"] */
       created_at timestamp with time zone NOT NULL ,
       /* ["legacy"]["screen_name"] */
       screen_name text NOT NULL ,
       /* ["legacy"]["name"] */
       full_name text ,
       /* ["legacy"]["description"] */
       profile_description text ,
       /* reconstructed from profile_descripton, URLs substituted by expanded_url */
       profile_input_description text NOT NULL ,
       /* ["legacy"]["entity"]["url"]["urls"][0]["expanded_url"] */
       profile_url text ,
       /* ["legacy"]["pinned_tweet_ids_str"][0] */
       pinned_id text ,
       /* ["legacy"]["followers_count"] */
       followers_count int ,
       /* ["legacy"]["friends_count"] */
       following_count int ,
       /* ["legacy"]["statuses_count"] */
       statuses_count int ,
       meta_inserted_at timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP ,
       meta_updated_at timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP ,
       meta_source text NOT NULL
) ;
/* CREATE UNIQUE INDEX authority_source_id ON authority ( meta_source , id ) ; */
CREATE INDEX authority_id_key ON authority ( id ) ;
CREATE INDEX tweets_author_key ON tweets ( author_id ) ;
CREATE INDEX tweets_author_screen_name_key ON tweets ( author_screen_name ) ;
CREATE INDEX tweets_conversation_key ON tweets ( conversation_id ) ;
CREATE INDEX tweets_thread_key ON tweets ( thread_id ) ;
CREATE INDEX tweets_replyto_key ON tweets ( replyto_id ) ;
CREATE INDEX tweets_replyto_author_key ON tweets ( replyto_author_id ) ;
CREATE INDEX tweets_replyto_screen_name_key ON tweets ( replyto_author_screen_name ) ;
CREATE INDEX tweets_retweeted_key ON tweets ( retweeted_id ) ;
CREATE INDEX tweets_retweeted_author_key ON tweets ( retweeted_author_id ) ;
CREATE INDEX tweets_retweeted_screen_name_key ON tweets ( retweeted_author_screen_name ) ;
CREATE INDEX tweets_quoted_key ON tweets ( quoted_id ) ;
CREATE INDEX tweets_quoted_author_key ON tweets ( quoted_author_id ) ;
CREATE INDEX tweets_quoted_screen_name_key ON tweets ( quoted_author_screen_name ) ;
CREATE INDEX media_parent_key ON media ( parent_id ) ;
CREATE INDEX media_short_url_key ON media ( short_url ) ;
CREATE INDEX media_display_url_key ON media ( display_url ) ;
CREATE INDEX users_screen_name_key ON users ( screen_name ) ;
