module.exports = {
    "DB": {
        "Type":"SYS_DATABASE_TYPE",
        "User":"SYS_DATABASE_POSTGRES_USER",
        "Password":"SYS_DATABASE_POSTGRES_PASSWORD",
        "Port":"SYS_SQL_PORT",
        "Host":"SYS_DATABASE_HOST",
        "Database":"SYS_DATABASE_POSTGRES_USER"
    },


    "Redis":
    {
        "ip": "SYS_REDIS_HOST",
        "port": "SYS_REDIS_PORT",
        "user": "SYS_REDIS_USER",
        "password": "SYS_REDIS_PASSWORD",
        "redisDB":"SYS_REDIS_DB_ARDS"

    },

    "Security":
    {
        "ip": "SYS_REDIS_HOST",
        "port": "SYS_REDIS_PORT",
        "user": "SYS_REDIS_USER",
        "password": "SYS_REDIS_PASSWORD"

    },

    "Mongo":
    {
        "ip":"SYS_MONGO_HOST",
        "port":"SYS_MONGO_PORT",
        "dbname":"SYS_MONGO_DB",
        "password":"SYS_MONGO_PASSWORD",
        "user":"SYS_MONGO_USER"
    },

    "RabbitMQ":
    {
        "ip": "SYS_RABBITMQ_HOST",
        "port": "SYS_RABBITMQ_PORT",
        "user": "SYS_RABBITMQ_USER",
        "password": "SYS_RABBITMQ_PASSWORD"
    },

    "Host":
    {
        "vdomain": "LB_FRONTEND",
        "domain": "HOST_NAME",
        "port": "HOST_LITETICKET_PORT",
        "version": "HOST_VERSION"
    },

    "LBServer" : {

        "ip": "LB_FRONTEND",
        "port": "LB_PORT"

    },
    "Services" : {
        "accessToken": "HOST_TOKEN",
        "resourceServiceHost": "SYS_RESOURCESERVICE_HOST",
        "resourceServicePort": "SYS_RESOURCESERVICE_PORT",
        "resourceServiceVersion": "SYS_RESOURCESERVICE_VERSION",
        "sipuserendpointserviceHost": "SYS_SIPUSERENDPOINTSERVICE_HOST",
        "sipuserendpointservicePort": "SYS_SIPUSERENDPOINTSERVICE_PORT",
        "sipuserendpointserviceVersion": "SYS_SIPUSERENDPOINTSERVICE_VERSION",
        "clusterconfigserviceHost": "SYS_CLUSTERCONFIG_HOST",
        "clusterconfigservicePort": "SYS_CLUSTERCONFIG_PORT",
        "clusterconfigserviceVersion": "SYS_CLUSTERCONFIG_VERSION",
        "ardsServiceHost": "SYS_ARDSLITESERVICE_HOST",
        "ardsServicePort": "SYS_ARDSLITESERVICE_PORT",
        "ardsServiceVersion": "SYS_ARDSLITESERVICE_VERSION",
        "notificationServiceHost": "SYS_NOTIFICATIONSERVICE_HOST",
        "notificationServicePort": "SYS_NOTIFICATIONSERVICE_PORT",
        "notificationServiceVersion": "SYS_NOTIFICATIONSERVICE_VERSION",
        "scheduleWorkerHost": "SYS_SCHEDULEWORKER_HOST",
        "scheduleWorkerPort": "SYS_SCHEDULEWORKER_PORT",
        "scheduleWorkerVersion": "SYS_SCHEDULEWORKER_VERSION",
        "interactionServiceHost": "SYS_INTERACTIONS_HOST",
        "interactionServicePort": "SYS_INTERACTIONS_PORT",
        "interactionServiceVersion": "SYS_INTERACTIONS_VERSION"
    }
};

//NODE_CONFIG_DIR
