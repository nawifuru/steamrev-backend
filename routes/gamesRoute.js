const express = require('express');
const router = express.Router();
const connectionSettings = require('../connectionSettings');
const { Pool } = require('pg');
const { check, validationResult } = require('express-validator');
const pool = new Pool(connectionSettings);

let gameList = [];
let popularGames = {
    total: [],
    recent: []
}
let metrics = {
    years: [],
    pricepoints: [],
    genres: [],
    categories: [],
    medianReview: [],
    medianRevenue: [],
    above10k: [],
    above50k: [],
    above200k: [],
    above500k: [],
    gameCount: null
}
module.exports = {
    Init: async function () {
        gameList = await GetAllGames();
        popularGames.total = await GetMostPopularGames();
        popularGames.recent = await GetRecentPopularGames();
        metrics.years = await GetDataYears(5);
        metrics.pricepoints = await GetPricepointMetrics(5);
        metrics.genres = await GetGenreMetrics(5);
        metrics.categories = await GetCategoryMetrics(5);
        metrics.medianReview = await GetMedianReviewCount(5);
        metrics.medianRevenue = await GetMedianRevenue(5);
        metrics.above10k = await GetNumOfGamesAboveRevenue(1000000, 5);
        metrics.above50k = await GetNumOfGamesAboveRevenue(5000000, 5);
        metrics.above200k = await GetNumOfGamesAboveRevenue(20000000, 5);
        metrics.above500k = await GetNumOfGamesAboveRevenue(50000000, 5);
        metrics.gameCount = await GetGameCount(5);
    },
    Router: function () {
        router.get('/', (req, res) => {
            res.send(gameList);
        });
        router.get('/popular', (req, res) => {
            res.send(popularGames);
        });
        router.get('/metrics', (req, res) => {
            res.send(metrics);
        });
        router.get('/quality', check('percentile').isFloat(), async (req, res) => {
            const errors = validationResult(req);
            if (!errors.isEmpty()) {
                return res.status(422).jsonp(errors.array());
            }
            const percentile = req.query.percentile;
            const games = await GetGamesAroundRevenuePercentile(percentile);
            res.send(games);
        })
        router.get('/:appid', check('appid').isInt(), async (req, res) => {
            const errors = validationResult(req);
            if (!errors.isEmpty()) {
                return res.status(422).jsonp(errors.array());
            }
            const results = await GetGameDetails(req.params.appid);
            res.send(results);
        });
        return router;
    }
}
async function GetGamesAroundRevenuePercentile(percentile) {
    const query = `SELECT appid, name, header_image, initial_price, final_price, discount_percent, total_reviews FROM
    ((SELECT * FROM game_revenue WHERE total_percentile >= ${percentile} ORDER BY total_percentile ASC LIMIT 15)
    UNION
    (SELECT * FROM game_revenue WHERE total_percentile < ${percentile} ORDER BY total_percentile DESC LIMIT 15))gameList
    LEFT JOIN game_details USING (appid)
    ORDER BY total_percentile ASC`;
    const results = await pool.query(query);
    return results.rows;
}
async function GetAllGames() {
    const query = `SELECT appid, name, header_image FROM game_details
    WHERE type = 'game'
    ORDER BY appid`;
    const results = await pool.query(query);
    return results.rows;
}
async function GetGameDetails(appid) {
    const query = `SELECT * FROM game_details
    LEFT JOIN game_revenue USING (appid)
    CROSS JOIN (SELECT ARRAY_AGG(description) AS genres FROM game_genres WHERE appid = $1)genres
    CROSS JOIN (SELECT ARRAY_AGG(description) AS categories FROM game_categories WHERE appid = $1)categories
    WHERE appid = $1`;
    const values = [appid];
    const results = await pool.query(query, values);
    return results.rows[0];
}
async function GetMostPopularGames() {
    const query = `SELECT appid, name, header_image, total_reviews, initial_price, final_price, discount_percent, release_date, revenue FROM
    (SELECT *, total_reviews * initial_price::bigint AS revenue FROM game_details)gameList
    WHERE revenue IS NOT NULL
    ORDER BY revenue DESC
    LIMIT 20`;
    const results = await pool.query(query);
    return results.rows;
}
async function GetRecentPopularGames() {
    const query = `SELECT appid, name, header_image, total_reviews, initial_price, final_price, discount_percent, release_date, revenue FROM
    (SELECT *, total_reviews * initial_price::bigint AS revenue FROM game_details)gameList
    WHERE revenue IS NOT NULL
    AND release_date > CURRENT_DATE - INTERVAL '1 months'
    ORDER BY revenue DESC
    LIMIT 20`;
    const results = await pool.query(query);
    return results.rows;
}
async function GetMedianRevenue(numOfYears) {
    const queryTemplate = (arg) => `SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_reviews * initial_price::bigint) FROM game_details
    WHERE type = 'game'
    AND total_reviews >= 10
    AND initial_price IS NOT NULL`;
    const results = await IterateQueryByYears(queryTemplate, numOfYears, true);
    return results;
}
async function GetMedianReviewCount(numOfYears) {
    const queryTemplate = (arg) => `SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_reviews) FROM game_details
    WHERE type = 'game'
    AND total_reviews >= 10
    AND initial_price IS NOT NULL`;
    const results = await IterateQueryByYears(queryTemplate, numOfYears, true);
    return results;
}
async function GetPricepointMetrics(numOfYears) {
    const queryTemplate = (arg) => `SELECT * FROM (SELECT COUNT(initial_price), initial_price, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_reviews) FROM game_details
    WHERE initial_price IS NOT NULL
    AND total_reviews >= 10
    AND type = 'game'
    ${arg}
    GROUP BY initial_price)gameList
    WHERE count >= 32`;
    const results = await IterateQueryByYears(queryTemplate, numOfYears);
    return results;
}
async function GetGenreMetrics(numOfYears) {
    const queryTemplate = (arg) => `SELECT * FROM
    (SELECT COUNT(*), id, description, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_reviews) FROM game_details
    LEFT JOIN game_genres USING(appid)
    WHERE initial_price IS NOT NULL
    AND total_reviews >= 10
    AND type = 'game'
    AND id IS NOT NULL
    ${arg}
    GROUP BY id, description)gameList
    WHERE count >= 32
    ORDER BY percentile_cont DESC`;
    const results = await IterateQueryByYears(queryTemplate, numOfYears);
    return results;
}
async function GetCategoryMetrics(numOfYears) {
    const queryTemplate = (arg) => `SELECT * FROM
    (SELECT COUNT(*), id, description, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_reviews) FROM game_details
    LEFT JOIN game_categories USING(appid)
    WHERE initial_price IS NOT NULL
    AND total_reviews >= 10
    AND type = 'game'
    AND id IS NOT NULL
    ${arg}
    GROUP BY id, description)gameList
    WHERE count >= 32
    ORDER BY percentile_cont DESC`;
    const results = await IterateQueryByYears(queryTemplate, numOfYears);
    return results;
}
async function GetNumOfGamesAboveRevenue(revenueAmt, numOfYears) {
    const queryTemplate = (arg) => `SELECT COUNT(*) FROM game_details
    WHERE total_reviews * initial_price::bigint > ${revenueAmt}
    ${arg}`;
    const results = await IterateQueryByYears(queryTemplate, numOfYears, true);
    return results;
}
async function GetGameCount(numOfYears) {
    const queryTemplate = (arg) => `SELECT COUNT(*) FROM game_details
    WHERE type = 'game'
    ${arg}`;
    const results = await IterateQueryByYears(queryTemplate, numOfYears, true);
    return results;
}
async function GetDataYears(numOfYears) {
    const resJSON = [];
    const currentYear = new Date().getFullYear();
    for (let i = -1; i < numOfYears; i++) {
        if (i == - 1) {
            resJSON.push({
                value: 'total',
                label: 'Total'
            });
        }
        else {
            resJSON.push({
                value: (currentYear - i).toString(),
                label: (currentYear - i).toString()
            });
        }
    }
    return resJSON;
}
async function IterateQueryByYears(queryTemplate, numOfYears, returnSingleRow = false) {
    const currentYear = new Date().getFullYear();
    const resJSON = [];
    for (let i = -1; i < numOfYears; i++) {
        if (i == -1) {
            const query = queryTemplate('');
            const results = await pool.query(query);
            resJSON.push({
                year: 'total',
                data: returnSingleRow ? results.rows[0] : results.rows
            })
        }
        else {
            const arg = `AND DATE_PART('year', release_date) = ${currentYear - i}`;
            const query = queryTemplate(arg);
            const results = await pool.query(query);
            resJSON.push({
                year: (currentYear - i).toString(),
                data: returnSingleRow ? results.rows[0] : results.rows
            })
        }
    }
    return resJSON;
}