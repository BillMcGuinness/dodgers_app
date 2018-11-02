CREATE TABLE dbo.searches (
	id NVARCHAR(100) NOT NULL,
	search_date NVARCHAR(100) NOT NULL,
	completed_in DECIMAL(5,3),
	res_count INT,
	max_id NVARCHAR(100) NOT NULL,
	refresh_url NVARCHAR(200) NOT NULL,
	since_id NVARCHAR(100) NOT NULL,
	query NVARCHAR(100) NOT NULL,
	next_results NVARCHAR(200) NOT NULL
	PRIMARY KEY (id)
);

CREATE TABLE dbo.searches_hist (
	id NVARCHAR(100) NOT NULL,
	search_date NVARCHAR(100) NOT NULL,
	completed_in DECIMAL(5,3),
	res_count INT,
	max_id NVARCHAR(100) NOT NULL,
	refresh_url NVARCHAR(200) NOT NULL,
	since_id NVARCHAR(100) NOT NULL,
	query NVARCHAR(100) NOT NULL,
	next_results NVARCHAR(200) NOT NULL
);

CREATE TABLE dbo.users (
	id NVARCHAR(100) NOT NULL,
	time_zone NVARCHAR(100),
	profile_image_url NVARCHAR(100),
	translator_type NVARCHAR(100),
	profile_image_url_https NVARCHAR(100),
	profile_background_image_url NVARCHAR(100),
	profile_sidebar_fill_color NVARCHAR(100),
	description NVARCHAR(200),
	default_profile BIT,
	profile_background_image_url_https NVARCHAR(100),
	favourites_count INT,
	name NVARCHAR(100),
	profile_use_background_image BIT,
	contributors_enabled BIT,
	profile_text_color NVARCHAR(100),
	listed_count INT,
	notifications BIT,
	profile_background_tile BIT,
	tweet_count INT,
	verified BIT,
	geo_enabled BIT,
	screen_name NVARCHAR(100),
	profile_sidebar_border_color NVARCHAR(100),
	default_profile_image BIT,
	followers_count INT,
	utc_offset NVARCHAR(100),
	location NVARCHAR(100),
	lang NVARCHAR(10),
	profile_banner_url NVARCHAR(100),
	protected BIT,
	has_extended_profile BIT,
	follow_request_sent BIT,
	url NVARCHAR(100),
	profile_background_color NVARCHAR(100),
	is_translator BIT,
	following BIT,
	friends_count INT,
	profile_link_color NVARCHAR(100),
	created_at NVARCHAR(100),
	is_translation_enabled BIT
	PRIMARY KEY (id)
);

CREATE TABLE dbo.users_HIST (
	id NVARCHAR(100) NOT NULL,
	time_zone NVARCHAR(100),
	profile_image_url NVARCHAR(100),
	translator_type NVARCHAR(100),
	profile_image_url_https NVARCHAR(100),
	profile_background_image_url NVARCHAR(100),
	profile_sidebar_fill_color NVARCHAR(100),
	description NVARCHAR(200),
	default_profile BIT,
	profile_background_image_url_https NVARCHAR(100),
	favourites_count INT,
	name NVARCHAR(100),
	profile_use_background_image BIT,
	contributors_enabled BIT,
	profile_text_color NVARCHAR(100),
	listed_count INT,
	notifications BIT,
	profile_background_tile BIT,
	tweet_count INT,
	verified BIT,
	geo_enabled BIT,
	screen_name NVARCHAR(100),
	profile_sidebar_border_color NVARCHAR(100),
	default_profile_image BIT,
	followers_count INT,
	utc_offset NVARCHAR(100),
	location NVARCHAR(100),
	lang NVARCHAR(10),
	profile_banner_url NVARCHAR(100),
	protected BIT,
	has_extended_profile BIT,
	follow_request_sent BIT,
	url NVARCHAR(100),
	profile_background_color NVARCHAR(100),
	is_translator BIT,
	following BIT,
	friends_count INT,
	profile_link_color NVARCHAR(100),
	created_at NVARCHAR(100),
	is_translation_enabled BIT
);

CREATE TABLE dbo.places (
	id NVARCHAR(100) NOT NULL,
	country NVARCHAR(100),
	country_code NVARCHAR(100),
	full_name NVARCHAR(100),
	name NVARCHAR(100),
	place_type NVARCHAR(100),
	url NVARCHAR(200),
	bounding_box_type NVARCHAR(100),
	PRIMARY KEY (id)
);

CREATE TABLE dbo.places_HIST (
	id NVARCHAR(100) NOT NULL,
	country NVARCHAR(100),
	country_code NVARCHAR(100),
	full_name NVARCHAR(100),
	name NVARCHAR(100),
	place_type NVARCHAR(100),
	url NVARCHAR(200),
	bounding_box_type NVARCHAR(100)
);

CREATE TABLE dbo.places_coordinates (
	id NVARCHAR(100) NOT NULL,
	place_id NVARCHAR(100),
	place_coordinate_lat DECIMAL(15,10),
	place_coordinate_long DECIMAL(15,10),
	PRIMARY KEY (id),
	FOREIGN KEY (place_id) REFERENCES dbo.places(id)
);

CREATE TABLE dbo.places_coordinates_HIST (
	id NVARCHAR(100) NOT NULL,
	place_id NVARCHAR(100),
	place_coordinate_lat DECIMAL(15,10),
	place_coordinate_long DECIMAL(15,10)
);

CREATE TABLE dbo.tweets (
	id NVARCHAR(100) NOT NULL,
	search_id NVARCHAR(100),
	geo_lat DECIMAL(15,10),
	geo_long DECIMAL(15,10),
	geo_type NVARCHAR(20),
	created_at NVARCHAR(25),
	favorite_count INT,
	favorited BIT,
	place_id NVARCHAR(100),
	in_reply_to_screen_name NVARCHAR(100),
	in_reply_to_status_id NVARCHAR(100),
    in_reply_to_user_id NVARCHAR(100),
    is_quote_status BIT,
    lang NVARCHAR(10),
    retweet_count INT,
    retweeted BIT,
    retweeted_tweet_id NVARCHAR(100),
    source NVARCHAR(100),
    tweet_text NVARCHAR(200),
    truncated BIT,
    possibly_sensitive BIT,
    user_id NVARCHAR(100)
	PRIMARY KEY (id),
	FOREIGN KEY (search_id) REFERENCES dbo.searches(id),
	FOREIGN KEY (place_id) REFERENCES dbo.places(id)
);

CREATE TABLE dbo.tweets_HIST (
	id NVARCHAR(100) NOT NULL,
	search_id NVARCHAR(100),
	geo_lat DECIMAL(15,10),
	geo_long DECIMAL(15,10),
	geo_type NVARCHAR(20),
	created_at NVARCHAR(25),
	favorite_count INT,
	favorited BIT,
	place_id NVARCHAR(100),
	in_reply_to_screen_name NVARCHAR(100),
	in_reply_to_status_id NVARCHAR(100),
    in_reply_to_user_id NVARCHAR(100),
    is_quote_status BIT,
    lang NVARCHAR(10),
    retweet_count INT,
    retweeted BIT,
    retweeted_tweet_id NVARCHAR(100),
    source NVARCHAR(100),
    text NVARCHAR(200),
    truncated BIT,
    possibly_sensitive BIT,
    user_id NVARCHAR(100)
);

CREATE TABLE dbo.tweet_user_mention (
	id NVARCHAR(100) NOT NULL,
	user_id NVARCHAR(100) NOT NULL,
	tweet_id NVARCHAR(100) NOT NULL,
	start_index INT,
	end_index INT,
	PRIMARY KEY (id),
	FOREIGN KEY (user_id) REFERENCES dbo.users(id),
	FOREIGN KEY (tweet_id) REFERENCES dbo.tweets(id)
);

CREATE TABLE dbo.tweet_user_mention_HIST (
	id NVARCHAR(100) NOT NULL,
	user_id NVARCHAR(100) NOT NULL,
	tweet_id NVARCHAR(100) NOT NULL,
	start_index INT,
	end_index INT
);

CREATE TABLE dbo.media (
	id NVARCHAR(100) NOT NULL,
	display_url NVARCHAR(100),
	expanded_url NVARCHAR(200),
	media_url NVARCHAR(200),
	media_url_https NVARCHAR(200),
	large_height INT,
	large_width INT,
	large_resize  NVARCHAR(15),
	medium_height INT,
	medium_width INT,
	medium_resize NVARCHAR(15),
	small_height INT,
	small_width INT,
	small_resize NVARCHAR(15),
	thumb_height INT,
	thumb_width INT,
	thumb_resize NVARCHAR(15),
	source_status_id NVARCHAR(100),
	source_user_id NVARCHAR(100),
	type NVARCHAR(100),
	url NVARCHAR(200),
	PRIMARY KEY (id)
);

CREATE TABLE dbo.media_HIST (
	id NVARCHAR(100) NOT NULL,
	display_url NVARCHAR(100),
	expanded_url NVARCHAR(200),
	media_url NVARCHAR(200),
	media_url_https NVARCHAR(200),
	large_height INT,
	large_width INT,
	large_resize  NVARCHAR(15),
	medium_height INT,
	medium_width INT,
	medium_resize NVARCHAR(15),
	small_height INT,
	small_width INT,
	small_resize NVARCHAR(15),
	thumb_height INT,
	thumb_width INT,
	thumb_resize NVARCHAR(15),
	source_status_id NVARCHAR(100),
	source_user_id NVARCHAR(100),
	type NVARCHAR(100),
	url NVARCHAR(200)
);

CREATE TABLE dbo.tweet_media (
	id NVARCHAR(100) NOT NULL,
	media_id NVARCHAR(100) NOT NULL,
	tweet_id NVARCHAR(100) NOT NULL,
	start_index INT,
	end_index INT,
	PRIMARY KEY (id),
	FOREIGN KEY (media_id) REFERENCES dbo.media(id),
	FOREIGN KEY (tweet_id) REFERENCES dbo.tweets(id)
);

CREATE TABLE dbo.tweet_media_HIST (
	id NVARCHAR(100) NOT NULL,
	media_id NVARCHAR(100) NOT NULL,
	tweet_id NVARCHAR(100) NOT NULL,
	start_index INT,
	end_index INT
);