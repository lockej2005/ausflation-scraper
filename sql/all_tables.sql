CREATE TABLE products (
    id text,
    title text,
    price_value numeric,
    was_price_value numeric,
    save_value numeric,
    unit_price text,
    url text,
    image_url text,
    category text,
    page integer,
    scrapedAt text,
    uploaded_at text,
    vendor text
);


CREATE TABLE category_stats (
    category text,
    expected_products integer,
    actual_products integer,
    duplicates_removed integer,
    pages_scraped integer,
    updated_at text
);


CREATE TABLE scraping_runs (
    id integer,
    run_timestamp text,
    categories_attempted integer,
    categories_successful integer,
    total_products integer,
    cross_category_duplicates integer,
    completed_at text
);
