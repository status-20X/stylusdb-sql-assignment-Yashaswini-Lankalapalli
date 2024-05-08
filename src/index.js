// src/index.js

const parseQuery = require('./queryParser');
const readCSV = require('./csvReader');

async function executeSELECTQuery(query) {
// src/index.js at executeSELECTQuery

// Now we will have joinTable, joinCondition in the parsed query
const { fields, table, whereClauses, joinTable, joinCondition } = parseQuery(query);
let data = await readCSV(`${table}.csv`);

// Perform INNER JOIN if specified
if (joinTable && joinCondition) {
    const joinData = await readCSV(`${joinTable}.csv`);
    data = data.flatMap(mainRow => {
        return joinData
            .filter(joinRow => {
                const mainValue = mainRow[joinCondition.left.split('.')[1]];
                const joinValue = joinRow[joinCondition.right.split('.')[1]];
                return mainValue === joinValue;
            })
            .map(joinRow => {
                return fields.reduce((acc, field) => {
                    const [tableName, fieldName] = field.split('.');
                    acc[field] = tableName === table ? mainRow[fieldName] : joinRow[fieldName];
                    return acc;
                }, {});
            });
    });
}

// Apply WHERE clause filtering after JOIN (or on the original data if no join)
const filteredData = whereClauses.length > 0
    ? data.filter(row => whereClauses.every(clause => evaluateCondition(row, clause)))
    : data;
}

// src/index.js

// ...existing imports...

// Helper functions for different JOIN types
function performInnerJoin(/* parameters */) {
    // Logic for INNER JOIN
    // ...
}

function performLeftJoin(/* parameters */) {
    // Logic for LEFT JOIN
    // ...
}

function performRightJoin(/* parameters */) {
    // Logic for RIGHT JOIN
    // ...
}

async function executeSELECTQuery(query) {
    const { fields, table, whereClauses, joinType, joinTable, joinCondition } = parseQuery(query);
    let data = await readCSV(`${table}.csv`);

    // Logic for applying JOINs
    if (joinTable && joinCondition) {
        const joinData = await readCSV(`${joinTable}.csv`);
        switch (joinType.toUpperCase()) {
            case 'INNER':
                data = performInnerJoin(data, joinData, joinCondition, fields, table);
                break;
            case 'LEFT':
                data = performLeftJoin(data, joinData, joinCondition, fields, table);
                break;
            case 'RIGHT':
                data = performRightJoin(data, joinData, joinCondition, fields, table);
                break;
            // Handle default case or unsupported JOIN types
        }
    }

    // ...existing code for WHERE clause and field selection...
}

module.exports = executeSELECTQuery;