CREATE TABLE products (
    product_no INT UNIQUE,
    name TEXT,
    price DECIMAL CHECK (price > 0),
    discounted_price DECIMAL CONSTRAINT positive_discount CHECK (discounted_price > 0),
    CHECK (product_no > 1),
    CONSTRAINT valid_discount CHECK (price > discounted_price)
)
