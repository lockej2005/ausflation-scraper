- Remove storing string value of price
- Make int value save in $ format (without $ sign)
- Save to Supabase and create db
- Remove Screenshots
- Componeratise parts where possible, but not excessively.

-- DONE -- 

- Make it update product, but with each save put it in db for history. Only uniqueness is id and date/time
- Remove RLS, only super admin can edit but anyone can view

- Make RLS anyone can view products, move db call to front end
- Add stock chart etc