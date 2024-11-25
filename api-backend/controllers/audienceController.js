const { getDB } = require("../config/db");

const validateRules = (rules) => {
    if (!Array.isArray(rules)) throw new Error("Rules must be an array.");
    rules.forEach((rule, index) => {
        if (!rule.field || !rule.operator || rule.value === undefined || !rule.condition) {
            throw new Error(`Rule at index ${index} is missing required properties.`);
        }
    });
};

const parseValue = (field, value) => {
    switch (field) {
        case "last_visit":
            return new Date(value);
        case "visits":
            return parseInt(value, 10);
        case "total_spends":
            return parseFloat(value);
        default:
            return value;
    }
};

const getMongoOperator = (operator, value, isDate) => {
    const ops = {
        ">": { $gt: value },
        "<": { $lt: value },
        "=": isDate ? { $gte: value.startOfDay, $lt: value.endOfDay } : value,
        "!=": { $ne: value },
        ">=": { $gte: value },
        "<=": { $lte: value },
    };
    return ops[operator];
};

const buildConditions = (rules) => {
    const combinedConditions = [];

    rules.forEach((rule) => {
        const value = parseValue(rule.field, rule.value);

        // Handle date-specific logic
        if (rule.field === "last_visit") {
            const startOfDay = new Date(value.setUTCHours(0, 0, 0, 0));
            const endOfDay = new Date(value.setUTCHours(23, 59, 59, 999));
            value.startOfDay = startOfDay;
            value.endOfDay = endOfDay;
        }

        const condition = { [rule.field]: getMongoOperator(rule.operator, value, rule.field === "last_visit") };

        // Group conditions by logical operator
        combinedConditions.push({ condition, logic: rule.condition });
    });

    return combinedConditions;
};

const combineConditions = (conditions) => {
    const query = {};

    // Check if any rule has the 'OR' condition
    const hasOrCondition = conditions.some((item) => item.logic === "OR");

    if (hasOrCondition) {
        query[Object.keys(conditions[0].condition)[0]] = Object.values(conditions[0].condition)[0];
    } else {
        // Apply all conditions using AND logic
        const andConditions = conditions.map((item) => item.condition);
        if (andConditions.length) query.$and = andConditions;
    }

    return query;
};

const getAudienceSizeHandler = async (rules) => {
    validateRules(rules);

    // Build and combine conditions dynamically
    const conditions = buildConditions(rules);
    const query = combineConditions(conditions);

    const db = getDB();
    try {
        return await db.collection("customers").countDocuments(query);
    } catch (error) {
        console.error("Error querying audience size:", error);
        throw new Error("Failed to calculate audience size.");
    }
};

const getAudienceSize = async (req, res) => {
    const { rules } = req.body;
    try {
        const audienceSize = await getAudienceSizeHandler(rules);
        res.json({ size: audienceSize });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
};

module.exports = { getAudienceSize, getAudienceSizeHandler };