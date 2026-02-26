-- nested_json_pizza_sample_data.json
-- { "Name": "BBQ Chicken", "Sizes": [{ "Size": "Large", "Price": 14.99 }, { "Size": "Medium", "Price": 12.99 }], "Toppings": [ "Barbecue Sauce", "Chicken", "Cheese" ] }
-- { "Name": "Hawaiian", "Sizes": [{ "Size": "Large", "Price": 12.99 }, { "Size": "Medium", "Price": 10.99 }], "Toppings": [ "Ham", "Pineapple", "Cheese" ] }
-- { "Name": "Vegetable", "Sizes": [{ "Size": "Large", "Price": 12.99 }, { "Size": "Medium", "Price": 10.99 }], "Toppings": [ "Broccoli", "Tomato", "Cheese" ] }
-- { "Name": "Pepperoni", "Sizes": [{ "Size": "Large", "Price": 12.99 }, { "Size": "Medium", "Price": 10.99 }, { "Size": "Small", "Price": 7.49 }], "Toppings": [ "Pepperoni", "Cheese" ] }
-- { "Name": "Cheese", "Sizes": [{ "Size": "Large", "Price": 10.99 }, { "Size": "Medium", "Price": 9.99 }, { "Size": "Small", "Price": 5.49 }], "Toppings": [ "Cheese" ] }

register /path/to/json-simple.jar;
register /path/to/elephant-bird-core.jar;
register /path/to/elephant-bird-pig.jar;

json_data = load '/path/to/nested_json_pizza_sample_data.json' using com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad');
toppings = foreach json_data generate flatten($0#'Toppings');
distinct_toppings = distinct toppings;
dump distinct_toppings;
