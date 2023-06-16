# Perl scripts for putting Twitter data and JSON in PostgreSQL #

by [David A. Madore](http://www.madore.org/~david/)


## What's this about? ##

Since Twitter [has
announced](https://twitter.com/TwitterDev/status/1621026986784337922)
that they're removing all free access to their API (although they
have, since that announcement, extended the deadline and conceded a
free *write-only* access which isn't terribly useful), I have been
developing this set of Perl scripts to

* process JSON data (for tweets and users) as it is sent by the
  Twitter servers to the Twitter web client,

* store it in a PostgreSQL database for later analysis.

I use it to maintain a comprehensive archive of the tweets I wrote (as
well as some others that are of interest to me) but it could be used
for various other purposes.

So all these scripts do is take JSON data (see below for an
explanation of how to capture this using Firefox) and store it in a
PostgreSQL database:

* extracting useful data (tweet id, creation date, author id, author
  screen name, textual content, parent / retweeted / quoted id, etc.)
  in easily accessible columns of a PostgreSQL table,

* but also storing the JSON itself, so various kinds of data not
  stored in the table can still be examined.

What you do with the data is then up to you.  The scripts provided
here are only about storing, not about querying (so you had better
know about PostgreSQL if you want to do anything useful with this).

**Note:** This is a very preliminary version, written in haste as
Twitter had announced they were closing their API shortly.  The code
is atrociously ugly (and very verbose as I [lacked the time to make it
short](https://mobile.twitter.com/jack/status/227875897749950464)).
Also (despite my feeble attempts at making things at least somewhat
robust), things are likely to break when Twitter changes the way they
format their JSON internally.  But that's all right, if this code is
still in use long in the future [we'll have bigger
problems](https://xkcd.com/2730/).  Anyway, my point is, I wrote this
for myself, I'm sharing it in the hope that it will be useful for
other people, but without any warranty, yada yada: if it breaks your
computer or demons fly out of your nose, that's your problem.

Also, please be aware that this is *slow* as hell (ballbark value:
insertion at around 50 tweets per second, which is pathetic).  I'm
not concerned about why this is at the moment.

Also also, I hereby put this code in the Public Domain.  (You're still
welcome to credit me if you reuse it in a significant way and feel
like not being an assh•le.)


## Quick start ##

Before you can start using these scripts, you need a running
[PostgreSQL](https://www.postgresql.org/) server (version 11 is
enough, probably some lower versions will work), Perl 5 (version 5.28
is enough, probably some lower versions will work) and the Perl
modules `JSON::XS` (Debian package `libjson-xs-perl`), `DBI` (Debian
package `libdbi-perl`) and `DBD::Pg` (Debian package
`libdbd-pg-perl`).

Create a database called `twitter` and initiate it with the SQL
commands in `twitter-schema.sql` as follows:

```
LC_ALL=C.UTF-8 createdb twitter
psql twitter -f twitter-schema.sql
```

(replace `twitter` by whatever name you want to give the database but
then use the `-d myname` option to all the scripts below).

Now capture some JSON data from Twitter as follows, using Firefox (I
assume Chrome has something similar, I don't know, I don't use it):

* open a new tab, open the Firefox dev tools (ctrl-shift-K), select
  the “network” tab, enter “`graphql`” in the filter bar at top (the
  one with a funnel icon), then (in the tab for which the dev tools
  have been opened), go to the URL of a Twitter search, e.g.,
  [`https://twitter.com/search?q=from%3A%40gro_tsen+include%3Anativeretweets&src=typed_query&f=live`](https://twitter.com/search?q=from%3A%40gro_tsen+include%3Anativeretweets&src=typed_query&f=live)
  to search for tweets by user `@gro_tsen`, and scroll down as far as
  desired: this should accumulate a number of requests to
  `SearchTimeline` in the network tab;

* click on the gear (⚙︎) icon at the top right and choose “Save All As
  HAR” from the menu, and save as a `.har` file somewhere (note that
  this file's name will be retained in the database to help tracing
  tweet data to their source requests, so maybe make it something
  memorable);

* run `insert-json.pl -h /path/to/the/file.har` to populate the
  database with the captured data;

* repeat as necessary for all the data you wish to capture.

The scripts will try to insert in the database whatever they can make
sense of that's provided in Twitter's JSON responses: for example,
insofar as Twitter provides information about quoted tweets, quoted
tweets will be inserted in the database (and not just the quoting
tweet).

If you wish to capture a specific thread or your profile's “tweets” or
“likes”, or your timeline, the same instructions apply: enter
“`graphql`” in the filter bar, view the tweet, profile or timeline
that you wish to save, then run `insert-json.pl` on the HAR file.

If you wish to import data from a [Twitter
archive](https://twitter.com/gro_tsen/status/1623376287308910605)
(accessible through “Download an archive of your data”), extract the
archive somewhere and run `insert-archive.pl
/path/to/the/archive/data/tweets.js` with environment variables
`TWEETS_ARCHIVE_USER_ID` and `TWEETS_ARCHIVE_USER_SCREEN_NAME` giving
you Twitter's account's id and screen name (=at-handle, but without
the leading `@` sign); if you don't know what they are, they are in
the fields `accountId` and `username` inside `account.js` in the same
archive.  Note however that the archive is very limited in the data it
contains (or I wouldn't have bothered to write this whole mess!),
e.g., it doesn't even contain the reference to the retweeted id for
retweets.


## Database format ##

The SQL schema is in `twitter-schema.sql` (and this should be used to
create the database).  Here are some comments about it.

* The JSON data for tweets (as well as user accounts, and media) is
  stored almost verbatim in the `authority` table.

  * The `id` column stores the id of the stored object, irrespective
    of its type.  The `obj_type` column stores the type as text:
    `tweet`, `media` or `user` as the case may be.

  * Unlike the other tables, this table *will not* enforce a
    uniqueness constraint on the `id` field alone: indeed, it can be
    desirable to store several versions of the JSON for the same
    object since they have different structures and aren't
    straightforwardly interconvertible; instead, the `authority`
    enforces uniqueness on `id` together with the `meta_source`
    column: this is initialized to `json-feed` by `insert-json.pl`, to
    `json-feed-v1` by `insert-json-v1.pl`, and to `tweets-archive` by
    `insert-archive.pl`, so basically a JSON version stored by each
    program can coexist in the database, but these are only default
    values and can be overridden by the `-s` option (so if you don't
    care about being able to store several different JSON versions of
    the same tweet, you might run everything with `-s json-feed` or
    whatever).

  * The columns `meta_inserted_at` and `meta_updated_at` give the time
    at which that particular line (i.e., `id` + `meta_source`
    combination) were first inserted and last updated respectively *in
    the database*.  (This has nothing to do with the date of the
    object being stored.)

  * The columns `auth_source` and `auth_date` give the source filename
    and date of the file *from which* the data was taken.  In the case
    of a HAR archive (`-h` option), `auth_date` is taken from the
    request start date of the request in whose response the data was
    found.

  * The `orig` column contains the JSON returned by Twitter (stored
    with the PostgreSQL `jsonb` type, so that it is [easily
    manipulable](https://www.postgresql.org/docs/current/functions-json.html)).
    However, only `insert-json.pl` stores it as such:
    `insert-json-v1.pl` and `insert-archive.pl` create a light wrapper
    where the JSON actually returned by Twitter is in the `legacy`
    field of the JSON, so that the different flavors are more or less
    compatible with one another (e.g., the tweet's full text is in
    `orig->'legacy'->>'full_text'` but note that it may not be
    *exactly* identical across JSON versions).

  * If the same object is re-inserted into the database, it *replaces*
    the former version in the `authority` table having the same
    `meta_source` value, if this version exists.  However, if the `-w`
    (“weak”) option was passed to the script, in which case already
    existing versions are left unchanged instead of being replaced
    (use this, e.g., when inserting data from tweets archive which is
    older than some data already present in the database).

* The most interesting tweet data is extracted from JSON into the
  `tweets` table.

  * Unlike `authority`, the `tweets` table stores just *one* version
    of each tweet (one line per tweet, uniqueness being ensured on the
    `id` column).  When re-inserting the same tweet several times, the
    newer values replace the older ones, except in the columns where
    the newer values are NULL in which case the older values are left
    untouched.  However, if the `-w` (“weak”) option was passed to the
    script (see above), only older NULL values are replaced.

  * The `id`, `created_at` and columns are the tweet's id and creation
    date.  I don't know how tweet edits work, so don't ask me: this is
    taken from the `created_at` field in the JSON (legacy part).

  * The `author_id` and `author_screen_name` columns are the tweet
    author's id and screen name: the reason why this is not simply
    cross-referenced to the `users` table is that accurate information
    about the tweet's author may not have been provided in the JSON.
    The same applies to similar other columns which may appear to be
    bad SQL design.

  * The `replyto_id`, `replyto_author_id` and
    `replyto_author_screen_name` refer to the id, author id and author
    screen name of the tweet to which the designated tweet is
    replying.  Similarly, `retweeted_id`, `retweeted_author_id` and
    `retweeted_author_screen_name` refer to the retweeted tweet if the
    designated tweet is a “native retweet” (note that native retweets
    are generally made invisible by Twitter, and replaced by the
    retweeted tweet whenever displayed).  Finally, `quoted_id`,
    `quoted_author_id` and `quoted_author_screen_name` refer to the
    quoted tweet if the designated tweet is a quote tweet (which may
    happen either because it was created by the quote tweet action or
    simply by pasting the URL of the tweet to be quoted; sadly, these
    two modes of quoting do not produce identical JSON in all
    circumstances).

  * The `full_text` column contains the tweet's text as it is stored
    in the `full_text` field in the JSON (legacy part): this is the
    text with URLs replaced by a shortened version.  As this is often
    not very useful, an attempt has been made, in the `input_text`
    column, to provide the text of the tweet *as it was input* instead
    (as nearly as it can be reconstituted): this consists of replacing
    URLs by their full version, and undoing the HTML-quoting of the
    tweet (so that, for example, `&` will appear as such and not as
    `&amp;`).  Note, however, that `input_text` cannot accurately
    reflect attached tweet media and quoted tweets: they are
    represented by URLs (but not necessarily in a consistent order if
    both are present, because Twitter doesn't seem to do things in a
    consistent way across JSON versions); and tweet cards (e.g.,
    polls) are dropped entirely from the `input_text` column (nor are
    they in `full_text`).

  * The `lang` column contains the (auto-detected) language of the
    tweet, with the string `und` (not NULL) marking an unknown
    language.

  * The columns `favorite_count`, `retweet_count`, `quote_count` and
    `reply_count` are the number of likes, retweets, quotes and
    retweets (as provided by the last JSON version seen for this
    tweet!).

  * The columns `meta_inserted_at` and `meta_updated_at` give the time
    at which that particular tweet was first inserted and last updated
    respectively *in the database*.  (This has nothing to do with the
    date of the tweet itself, which is stored in `created_at` as
    explained earlier.)  Note that `meta_updated_at` should coincide
    with the corresponding column in the `authority` table for the
    most recently updated line there, and the `meta_souce` column in
    `tweets` should coincide with `meta_source` for that same line in
    the `authority` table, but it's probably best not to rely on this
    as the logic may be flaky (and it was arguably a mistake to have
    these columns in the first place).

* The `media` table contains information as to tweet media.

  * The `id` column is the media id, whereas `parent_id` is the id of
    the parent tweet (the parent tweet appears to be the tweet which
    included the media in the first place; there is nothing forbidding
    reuse of media in later tweets), and `parent_tweet_author_id` and
    `parent_tweet_author_screen_name` refer to the parent tweet's
    author (which can be considered the media's author) in the same
    logic as for various `*_author_id` and `*_author_screen_name`
    columns in the `tweets` table, see above.

  * The `short_url` is the media's shortened (`t.co`) URL as it should
    appear in a tweet's `full_text`; the `display_url` is apparently
    the same but starts in `pic.twitter.com` instead of
    `https://t.co`; the `media_url` is the HTTPS link to the file on
    `pbs.twimg.com/media` where the image actual resides (at least for
    images).

  * The `media_type` is either “`photo`” or “`video`” or
    “`animated_gif`” as far as I can tell.

  * The `alt_text` column contains the image's alt-text.  This was my
    main motivation for having the `media` table at all (and, to some
    extent, this entire database system, as this information is not
    provided in the data archive's `tweets.js` file).

  * The columns `meta_inserted_at`, `meta_updated_at` and
    `meta_source` for media follow the same logic as for tweets, see
    above.

* The `users` table contains information as to user accounts
  encountered.

  * The column `id` is the account's numeric id (also known as “REST
    id”); Twitter has another id which is *not* included as a database
    column as I don't know what it's used for (but you can find it in
    the `id` field of the JSON data in the form that's handled by
    `insert-json.pl`).

  * The `created_at` column is the account's creation date.

  * The `screen_name` column is the account's at-handle (without the
    leading `@` sign)), whereas `full_name` refers to the (free form)
    displayed name.

  * The `profile_description` and `profile_input_description` follow
    the same logic as `full_text` and `input_text` in the `tweets`
    table: the former simply reflects the corresponding field in JSON
    (so it has shortened URLs) whereas the latter substitutes these
    shortened URLs for the links they stand for.  (Note that
    `profile_description` in `users` is *not* HTML-encoded unlike
    `full_text` in `tweets` which *is*, this is not my choice, that's
    how Twitter provides it.)

  * The `pinned_id` column is the id of the pinned tweet.  The
    `followers_count`, `following_count` and `statuses_count` are the
    numbers of followers, followees (“friends”) and tweets (as
    provided by the last JSON version seen for this user!).

  * The columns `meta_inserted_at`, `meta_updated_at` and
    `meta_source` for media follow the same logic as for tweets, see
    above.


## Running the scripts ##

There are three (well, four) versions of basically the same script,
which sadly share a lot of code with various differences because
Twitter can return the same data in *similar but subtly different
ways*.

* The `insert-json.pl` version of the script handles JSON returned by
  “`graphql`” type requests such as `TweetDetail`, `UserTweets`,
  `SearchTimeline` and so on.  As explained earlier, it saves results
  in the `authority` table with `meta_source` equal to `json-feed`
  (this can be overridden with the `-s` option if you're not happy).

* The `insert-json-v1.pl` version of the script was formerly uesd to
  handle JSON returned by “`adaptive`” type requests, in other words,
  search results.  Sometime around 2023-06, Twitter changed the format
  of `adaptive` requests and now encapsulates them as the others,
  making the use of this script seemingly obsolete.  As explained
  earlier, it saves results in the `authority` table with
  `meta_source` equal to `json-feed-v1` (this can be overridden with
  the `-s` option if you're not happy).

* The `insert-archive.pl` version of the script handles the
  `tweets.js` (as well as `deleted-tweets.js` but I didn't test this)
  contained in a GDPR-mandated “archive of your data”.  Note that
  since the `tweets.js` file does not recall the account's id and
  screen name, this must be provided through the
  `TWEETS_ARCHIVE_USER_ID` and `TWEETS_ARCHIVE_USER_SCREEN_NAME`
  environment variables.  As explained earlier, this script saves
  results in the `authority` table with `meta_source` equal to
  `tweets-archive` (this can be overridden with the `-s` option if
  you're not happy).

* Do not attempt to use `insert-daml.pl` or `generate-daml.pl`, they
  are for my own internal use and will likely be of no use to anyone
  else (the first parses HTML from a pre-existing archive of my tweets
  and the second generates an archive in such a format).

All versions of the script allow a `-d` option to specify the database
to which the script should connect (to specify the PostgreSQL server
and so on, use the standard environment variables such as `PGHOST`,
`PGPORT`, `PGUSER`: see `DBD::Pg(3pm)` for details).

All versions of the script allow a `-s` option to specify the
`meta_source` value with which to label data in the `authority` table.
The `auth_source` is taken from the input file name, and `auth_date`
from the input file's modification date except for HAR archives where
it is taken from the start date of the request in whose response the
data was found.

All versions of the script allow a `-w` option, requesting “weak”
mode: in “weak” mode, already existing data in the database takes
precedence over data inside the file being parsed: the latter will be
used only to replace NULL values (or nonexistent lines in the
`authority` table).

The scripts `insert-json.pl` and `insert-json-v1.pl` have a `-h`
option to import data from a HAR dump.  They will take whatever JSON
data they think they can understand from the HAR dump.  Yes, this
option should have been called something else, and no, there is no
“help” option: you're reading whatever help there is.  Without the
`-h` option, the script expects the JSON response itself as input.

For those who wish to do the same, a description of the process I use
to collect my own tweets is [given
here](https://twitter.com/gro_tsen/status/1628773723175112704).
