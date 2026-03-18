-- Run this in the Supabase SQL Editor (supabase.com → your project → SQL Editor)

create table posts (
  id bigint generated always as identity primary key,
  company_name text not null,
  post_url text unique not null,
  post_text text,
  posted_date text,
  likes integer default 0,
  comments integer default 0,
  views integer default 0,
  prev_likes integer default 0,
  likes_increased boolean default false,
  flagged boolean default false,
  image_url text,
  first_seen_at timestamptz default now(),
  last_updated_at timestamptz default now()
);

create table profiles (
  id bigint generated always as identity primary key,
  linkedin_url text unique not null,
  full_name text,
  title text,
  company text,
  email text,
  first_seen_at timestamptz default now(),
  last_updated_at timestamptz default now()
);

create table engagement (
  id bigint generated always as identity primary key,
  post_id bigint references posts(id) on delete cascade,
  profile_id bigint references profiles(id) on delete cascade,
  engagement_type text check (engagement_type in ('like', 'comment')),
  engaged_at timestamptz,
  created_at timestamptz default now(),
  unique(post_id, profile_id, engagement_type)
);

create table engagement_history (
  id bigint generated always as identity primary key,
  post_id bigint references posts(id) on delete cascade,
  likes integer,
  comments integer,
  views integer,
  recorded_at timestamptz default now()
);
