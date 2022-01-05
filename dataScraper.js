const connectionSettings = require('./connectionSettings');
const { Pool } = require('pg');
const { default: axios } = require('axios');
const pool = new Pool(connectionSettings);

module.exports = {
    UpdateApplist: async function () {
        try {
            const results = await axios.get('https://api.steampowered.com/ISteamApps/GetAppList/v2/');
            for (let game of results.data.applist.apps) {
                const query = `INSERT INTO games(appid, name) VALUES($1, $2) ON CONFLICT DO NOTHING`;
                const values = [game.appid, game.name];
                await pool.query(query, values);
                console.log(`Inserted appid (${game.appid}) => ${game.name} into the DB.`);
            }
        }
        catch (err) {
            console.log(err);
        }
    },
    UpdateAppDetails: async function () {
        try {
            const apps = await GetRemainingAppsFromDB();
            console.log(`Apps remaining: ${apps.length}`);
            for (let app of apps) {
                await Sleep(1300);
                console.log(`Operation started for ${app.appid} => ${app.name}...`);
                const results = await axios.get(`https://store.steampowered.com/api/appdetails/?appids=${app.appid}&cc=us`);
                //axios will throw error if request failed, we can simply check for null without concern for handling failed requests wrongly.
                if (results.data[app.appid]?.success == false || results.data[app.appid]?.data == null) {
                    await InsertAppIntoDB(app, null);
                    continue;
                }
                else if (results.data[app.appid]?.success == true && results.data[app.appid]?.data != null) {
                    const details = SanitizeData(results.data[app.appid].data);
                    await InsertAppIntoDB(app, details);
                    continue;
                }
                else {
                    this.UpdateAppDetails();
                    return;
                }
            }
        }
        catch (err) {
            console.log(err);
            this.UpdateAppDetails();
            return;
        }
    },
    UpdateAppReviews: async function (initialIndex) {
        let counter = 0;
        try {
            let games = await GetGamesFromDB();
            games = games.slice(initialIndex);
            console.log(`Apps remaining: ${games.length}`);
            for (let game of games) {
                await Sleep(1300);
                console.log(`Operation started for index => ${initialIndex + counter}`);
                const results = await axios.get(`https://store.steampowered.com/appreviews/${game.appid}?json=1&num_per_page=0&language=all`);
                //axios will throw error if request failed, we can simply check for null without concern for handling failed requests wrongly.
                if (results.data?.success == false || results.data?.query_summary == null) {
                    counter++;
                    continue;
                }
                else if (results.data?.success == true && results.data?.query_summary != null) {
                    await UpdateAppReviewsInDB(game, results.data.query_summary);
                    counter++;
                    continue;
                }
                else {
                    this.UpdateAppReviews(initialIndex + counter);
                    return;
                }
            }
        }
        catch (err) {
            console.log(err);
            this.UpdateAppReviews(initialIndex + counter);
            return;
        }
    },
    UpdateAppMetrics: async function () {
        try {
            const args = [
                {
                    identifier: 'low',
                    value: 'revenue < (SELECT * FROM lowThreshold)'
                },
                {
                    identifier: 'med',
                    value: 'revenue >= (SELECT * FROM lowThreshold) AND revenue <= (SELECT * FROM highThreshold)'
                },
                {
                    identifier: 'high',
                    value: 'revenue > (SELECT * FROM highThreshold)'
                }
            ]
            for (let arg of args) {
                const tierQuery = `WITH highThreshold AS (SELECT PERCENTILE_CONT(0.8) WITHIN GROUP(ORDER BY total_reviews * initial_price::bigint) FROM game_details
                WHERE total_reviews >= 10
                AND initial_price IS NOT NULL
                AND type ='game'),
                lowThreshold AS (SELECT PERCENTILE_CONT(0.64) WITHIN GROUP(ORDER BY total_reviews * initial_price::bigint) FROM game_details
                WHERE total_reviews >= 10
                AND initial_price IS NOT NULL
                AND type ='game')
                
                SELECT *, PERCENT_RANK() OVER(ORDER BY revenue) FROM
                (SELECT appid, name, total_reviews * initial_price::bigint AS revenue FROM game_details
                WHERE total_reviews >= 10
                AND initial_price IS NOT NULL
                AND type ='game')gameList
                WHERE ${arg.value}`;
                const tierResults = await pool.query(tierQuery);
                for (let game of tierResults.rows) {
                    const insertQuery = `INSERT INTO game_revenue(appid, tier, tier_percentile)
                    VALUES($1, $2, $3) ON CONFLICT(appid) DO UPDATE SET
                    tier = $2,
                    tier_percentile = $3`;
                    const insertValues = [
                        game.appid,
                        arg.identifier,
                        game.percent_rank
                    ];
                    await pool.query(insertQuery, insertValues);
                    console.log(`Updated tier revenue metrics of ${game.appid} => ${game.name}`);
                }
            }
            const totalQuery = `SELECT *, PERCENT_RANK() OVER(ORDER BY revenue) FROM
            (SELECT appid, name, total_reviews * initial_price::bigint AS revenue FROM game_details
            WHERE total_reviews >= 10
            AND initial_price IS NOT NULL
            AND type ='game')gameList`;
            const totalResults = await pool.query(totalQuery);
            for (let game of totalResults.rows) {
                const insertQuery = `UPDATE game_revenue SET total_percentile = $2 WHERE appid = $1`;
                const insertValues = [
                    game.appid,
                    game.percent_rank
                ];
                await pool.query(insertQuery, insertValues);
                console.log(`Updated total revenue metrics of ${game.appid} => ${game.name}`);
            }
        }
        catch (err) {
            console.log(err);
        }
    }
}
async function GetRemainingAppsFromDB() {
    const query = `SELECT games.appid, games.name FROM games
    LEFT JOIN game_details USING(appid)
    WHERE game_details.appid IS NULL
    ORDER BY games.appid`;
    const results = await pool.query(query);
    return results.rows;
}
async function GetGamesFromDB() {
    const query = `SELECT * FROM game_details WHERE type='game' ORDER BY appid`;
    const results = await pool.query(query);
    return results.rows;
}
async function InsertAppIntoDB(app, details) {
    const client = await pool.connect();
    try {
        await client.query('BEGIN');
        const query = `INSERT INTO game_details
        VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24)
        ON CONFLICT DO NOTHING`;
        const values = [
            app.appid,
            app.name,
            details?.type,
            details?.steam_appid,
            details?.required_age,
            details?.is_free,
            details?.detailed_description,
            details?.about_the_game,
            details?.short_description,
            details?.supported_languages,
            details?.header_image,
            details?.website,
            details?.developers,
            details?.publishers,
            details?.platforms?.windows,
            details?.platforms?.mac,
            details?.platforms?.linux,
            details?.metacritic?.score,
            details?.metacritic?.url,
            details?.price_overview?.initial,
            details?.price_overview?.final,
            details?.price_overview?.discount_percent,
            details?.recommendations?.total,
            details?.release_date?.date
        ];
        await client.query(query, values);
        if (details?.genres != null) {
            for (let genre of details.genres) {
                const genreQuery = `INSERT INTO game_genres(appid, id, description) VALUES($1, $2, $3) ON CONFLICT DO NOTHING`;
                const genreValues = [app.appid, genre.id, genre.description];
                await client.query(genreQuery, genreValues);
            }
        }
        if (details?.categories != null) {
            for (let category of details.categories) {
                const categoryQuery = `INSERT INTO game_categories(appid, id, description) VALUES($1, $2, $3) ON CONFLICT DO NOTHING`;
                const categoryValues = [app.appid, category.id, category.description];
                await client.query(categoryQuery, categoryValues);
            }
        }
        // if (details?.screenshots != null) {
        //     for (let screenshot of details.screenshots) {
        //         const screenshotQuery = `INSERT INTO game_screenshots(appid, id, path_thumbnail, path_full) VALUES($1, $2, $3, $4) ON CONFLICT DO NOTHING`;
        //         const screenshotValues = [app.appid, screenshot.id, screenshot.path_thumbnail, screenshot.path_full];
        //         await client.query(screenshotQuery, screenshotValues);
        //     }
        // }
        // if (details?.movies != null) {
        //     for (let movie of details.movies) {
        //         const movieQuery = `INSERT INTO game_movies(appid, id, name, thumbnail, webm_480, web_max, mp4_480, mp4_max, highlight)
        //         VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9) ON CONFLICT DO NOTHING`;
        //         const movieValues = [
        //             app.appid,
        //             movie.id,
        //             movie.name,
        //             movie.thumbnail,
        //             movie.webm[480],
        //             movie.webm?.max,
        //             movie.mp4[480],
        //             movie.mp4?.max,
        //             movie.highlight
        //         ];
        //         await client.query(movieQuery, movieValues);
        //     }
        // }
        await client.query('COMMIT');
        details ? console.log(`Inserted ${app.appid} => ${app.name} into the DB.`)
            : console.log(`Inserted NULL ROW ${app.appid} => ${app.name} into the DB.`);
    }
    catch (err) {
        console.log(err);
        await client.query('ROLLBACK');
    }
    finally {
        client.release();
    }
}
async function UpdateAppReviewsInDB(app, details) {
    const query = `UPDATE game_details SET
    num_reviews = $1,
    review_score = $2,
    review_score_desc = $3,
    total_positive = $4,
    total_negative = $5,
    total_reviews = $6
    WHERE appid = $7`;
    const values = [
        details.num_reviews,
        details.review_score,
        details.review_score_desc,
        details.total_positive,
        details.total_negative,
        details.total_reviews,
        app.appid
    ];
    await pool.query(query, values);
    console.log(`Updated app reviews of ${app.appid} => ${app.name}.`);
}
function SanitizeData(details) {
    if (details.release_date?.date != null) {
        if (isNaN(Date.parse(details.release_date.date))) {
            details.release_date.date = null;
        }
        else {
            details.release_date.date = new Date(details.release_date.date);
        }
    }
    return details;
}
function Sleep(ms) {
    return new Promise(res => setTimeout(res, ms))
}