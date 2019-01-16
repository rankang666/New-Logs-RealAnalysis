create database if not exists news_db;
use news_db;
create table b_topics_top(
  topic varchar(200),
  times int
);
create table b_hour_topics(
   hour varchar(10),
   times int
);
