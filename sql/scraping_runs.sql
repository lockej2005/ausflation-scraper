CREATE TABLE scraping_runs (
    id integer,
    run_timestamp text,
    categories_attempted integer,
    categories_successful integer,
    total_products integer,
    cross_category_duplicates integer,
    completed_at text
);
