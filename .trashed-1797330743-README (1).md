# Brewline: synthetic customer analytics dataset

Fictional online store selling coffee, tea, brewing equipment and accessories.
Fully synthetic (no real customers). Currency: USD. Data window: 2023-01-01 to 2026-10-31 (cutoff).

Files: 6 CSVs + `brewline.db` (the same data as a SQLite database, with indexes).

## Tables

**customers** (30,529): one row per registered account
customer_id, signup_date, acquisition_channel, country, device_type, marketing_opt_in (0/1)

**orders** (82,600): one row per order
order_id, customer_id, order_date, order_status (completed/cancelled), promo_code (NULL if none), shipping_fee, order_total (net items + shipping), payment_method

**order_items** (151,730): one row per product line in an order
order_item_id, order_id, product_id, quantity, unit_price (price at time of order), discount_amount (line total, not per unit)

**returns** (4,074): one row per returned order line
return_id, order_item_id, return_date, return_reason, refund_amount

**products** (48): product_id, product_name, category (Coffee/Tea/Equipment/Accessories), list_price, unit_cost

**marketing_spend** (276): monthly spend per channel
month (first day of month), channel, spend

## Things to know (real data is never clean)

- Some customers registered but never ordered.
- Cancelled orders exist and should usually be excluded from revenue and retention.
- Data stops at the cutoff date, so recent cohorts have had less time to behave. Handle this carefully.
- Prices and shipping rules change over time.
- order_total is in `orders`; line-level amounts must be derived from `order_items`.
- Channel names in `marketing_spend` match `acquisition_channel` in `customers`.
- Nothing here tells you the "answers". Finding the patterns is the portfolio work.
