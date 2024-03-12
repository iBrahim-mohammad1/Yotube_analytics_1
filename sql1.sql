1
select * from youtube_videos; 

2
SELECT 
video_id,
latest_by_offset(title) as title ,
latest_by_offset(comments , 2) as comments,
latest_by_offset(views ,2 ) as views,
latest_by_offset(likes ,2 ) as likes,
latest_by_offset(favorites) as favorites

FROM youtube_videos
GROUP BY video_id
EMIT CHANGES;


3
SELECT 
video_id,
latest_by_offset(title) as title ,
latest_by_offset(comments , 2)[2] as comments_curr,
latest_by_offset(comments , 2)[1] as comments_prev,
latest_by_offset(views ,2 )[2] as views_curr,
latest_by_offset(views ,2 )[1] as views_prev,
latest_by_offset(likes ,2 )[2] as likes_curr,
latest_by_offset(likes ,2)[1] as likes_prev,
latest_by_offset(favorites,2)[2] as favorites_curr,
latest_by_offset(favorites,2)[1] as favorites_prev

FROM youtube_videos
GROUP BY video_id
EMIT CHANGES;



4
SELECT 
video_id,
latest_by_offset(title) as title ,
latest_by_offset(comments , 2)[2] as comments_curr,
latest_by_offset(comments , 2)[1] as comments_prev,
latest_by_offset(views ,2 )[2] as views_curr,
latest_by_offset(views ,2 )[1] as views_prev,
latest_by_offset(likes ,2 )[2] as likes_curr,
latest_by_offset(likes ,2)[1] as likes_prev,
latest_by_offset(favorites,2)[2] as favorites_curr,
latest_by_offset(favorites,2)[1] as favorites_prev

FROM youtube_videos
GROUP BY video_id
EMIT CHANGES;


CREATE TABLE youtube_analytics_changes WITH(KAFKA_TOPIC='youtube_analytics_changes') AS
SELECT 
video_id,
latest_by_offset(title) as title ,
latest_by_offset(comments , 2)[2] as comments_curr,
latest_by_offset(comments , 2)[1] as comments_prev,
latest_by_offset(views ,2 )[2] as views_curr,
latest_by_offset(views ,2 )[1] as views_prev,
latest_by_offset(likes ,2 )[2] as likes_curr,
latest_by_offset(likes ,2)[1] as likes_prev,
latest_by_offset(favorites,2)[2] as favorites_curr,
latest_by_offset(favorites,2)[1] as favorites_prev

FROM youtube_videos
GROUP BY video_id;