/* search for various possible inconsistencies */
SELECT t.id , t2.id , t.replyto_author_id , t2.author_id FROM tweets t , tweets t2 WHERE t2.id=t.replyto_id AND t.replyto_author_id <> t2.author_id ;
SELECT t.id , t2.id , t.retweeted_author_id , t2.author_id FROM tweets t , tweets t2 WHERE t2.id=t.retweeted_id AND t.retweeted_author_id <> t2.author_id ;
SELECT t.id , t2.id , t.quoted_author_id , t2.author_id FROM tweets t , tweets t2 WHERE t2.id=t.quoted_id AND t.quoted_author_id <> t2.author_id ;
SELECT m.id , t2.id , m.parent_author_id , t2.author_id FROM media m , tweets t2 WHERE t2.id=m.parent_id AND m.parent_author_id <> t2.author_id ;

/* update date versus most recent authority */
SELECT t.id , t.meta_updated_at , max(a.meta_updated_at) AS maxdate FROM tweets t , authority a WHERE t.id=a.id GROUP BY t.id , a.id HAVING t.meta_updated_at<>max(a.meta_updated_at) ORDER BY t.id ASC ;
SELECT t.id , t.meta_updated_at , a.meta_updated_at FROM tweets t , authority a WHERE t.id=a.id AND t.meta_source=a.meta_source AND t.meta_updated_at<>a.meta_updated_at ORDER BY t.id ASC ;

/* search and fix various missing fields */
SELECT t.id , t2.id , t.replyto_author_id , t2.author_id FROM tweets t , tweets t2 WHERE t2.id=t.replyto_id AND t.replyto_author_id ISNULL AND t2.author_id NOTNULL ;
UPDATE tweets AS t SET replyto_author_id=t2.author_id FROM tweets AS t2 WHERE t2.id=t.replyto_id AND t.replyto_author_id ISNULL AND t2.author_id NOTNULL ;
SELECT t.id , t2.id , t.retweeted_author_id , t2.author_id FROM tweets t , tweets t2 WHERE t2.id=t.retweeted_id AND t.retweeted_author_id ISNULL AND t2.author_id NOTNULL ;
UPDATE tweets AS t SET retweeted_author_id=t2.author_id FROM tweets AS t2 WHERE t2.id=t.retweeted_id AND t.retweeted_author_id ISNULL AND t2.author_id NOTNULL ;
SELECT t.id , t2.id , t.quoted_author_id , t2.author_id FROM tweets t , tweets t2 WHERE t2.id=t.quoted_id AND t.quoted_author_id ISNULL AND t2.author_id NOTNULL ;
UPDATE tweets AS t SET quoted_author_id=t2.author_id FROM tweets AS t2 WHERE t2.id=t.quoted_id AND t.quoted_author_id ISNULL AND t2.author_id NOTNULL ;
SELECT t.id , t2.id , t.quoted_author_id , t2.quoted_author_id FROM tweets t , tweets t2 WHERE t2.id=t.retweeted_id AND t.quoted_author_id ISNULL AND t2.quoted_author_id NOTNULL ;
UPDATE tweets AS t SET quoted_author_id=t2.quoted_author_id FROM tweets AS t2 WHERE t2.id=t.retweeted_id AND t.quoted_author_id ISNULL AND t2.quoted_author_id NOTNULL ;
SELECT m.id , t2.id , m.parent_author_id , t2.author_id FROM media m , tweets t2 WHERE t2.id=m.parent_id AND m.parent_author_id ISNULL AND t2.author_id NOTNULL ;
UPDATE media AS m SET parent_author_id=t2.author_id FROM tweets AS t2 WHERE t2.id=m.parent_id AND m.parent_author_id ISNULL AND t2.author_id NOTNULL ;

/* search for tweets which don't have a proper JSON authority */
SELECT id , created_at FROM tweets WHERE author_id='1018078984280657920' AND retweeted_id ISNULL AND NOT EXISTS ( SELECT * FROM authority WHERE authority.id=tweets.id AND ( authority.meta_source='json-feed-v1' OR authority.meta_source='json-feed' ) ) ORDER BY created_at DESC LIMIT 200 ;
\copy (SELECT 'https://twitter.com/gro_tsen/status/'||id AS url FROM tweets WHERE author_id='1018078984280657920' AND retweeted_id ISNULL AND created_at>='2022-03-01'::timestamptz AND created_at<='2022-05-01' AND NOT EXISTS ( SELECT * FROM authority WHERE authority.id=tweets.id AND ( authority.meta_source='json-feed-v1' OR authority.meta_source='json-feed' ) ) ORDER BY created_at DESC) to '/tmp/missing.csv' csv ;
