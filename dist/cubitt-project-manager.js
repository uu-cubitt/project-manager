"use strict";
var Router = require("vertx-web-js/router");
var BodyHandler = require("vertx-web-js/body_handler");
var server = vertx.createHttpServer();
var eb = vertx.eventBus();
var router = Router.router(vertx);
var Common = require("cubitt-common");
var JDBCClient = require("vertx-jdbc-js/jdbc_client");
var sd = vertx.sharedData();
var postgresUser = process.env.POSTGRES_USER || "postgres";
var postgresPass = process.env.POSTGRES_PASSWORD || "";
var postgresDb = process.env.POSTGRES_DB || postgresUser;
var postgresHost = process.env.POSTGRES_HOST || "localhost";
var postgresPort = process.env.POSTGRES_PORT || 5432;
var client = JDBCClient.createShared(vertx, {
    "url": "jdbc:postgresql://" + postgresHost + ":" + postgresPort + "/" + postgresDb + "?user=" + postgresUser + "&password=" + postgresPass,
    "driver_class": "org.postgresql.Driver",
    "max_pool_size": 30
});
router.route().handler(BodyHandler.create().handle);
var createRoute = router.route("POST", "/projects/").consumes("application/json").produces("application/json");
createRoute.handler(function (routingContext) {
    var body = routingContext.getBodyAsJson();
    var response = routingContext.response();
    var id = Common.Guid.parse(body.id);
    if (id === null) {
        response.setStatusCode(400).end(JSON.stringify({ status: 400, data: null, error: "id should be a valid GUID" }));
        return;
    }
    var options = {
        "config": {
            "id": id
        }
    };
    sd.getClusterWideMap("actors", function (res, res_err) {
        if (res_err !== null) {
            console.log("Failed to obtain actor map: " + res_err);
            response.setStatusCode(500).end();
            return;
        }
        var actors = res;
        actors.putIfAbsent(id.toString(), true, function (res, res_err) {
            if (res_err !== null) {
                console.log("Failed to check/set value in actor map: " + res_err);
                response.setStatusCode(500).end();
                return;
            }
            if (res !== null) {
                console.log("Actor already exists");
                response.setStatusCode(409).end();
                return;
            }
            client.getConnection(function (conn, conn_err) {
                if (conn_err !== null) {
                    console.log("Error connecting to database: " + conn_err);
                    response.setStatusCode(500).end();
                    return;
                }
                var connection = conn;
                connection.execute("create table if not exists \"" + id.toString() + "_events\" (id integer NOT NULL primary key, event json NOT NULL)", function (res, res_err) {
                    if (res_err) {
                        console.log("Failed to create table: " + res_err);
                        response.setStatusCode(500).end();
                        return;
                    }
                    connection.close(function (done, done_err) {
                        if (done_err) {
                            console.log("Failed to close connection: " + done_err);
                            response.setStatusCode(500).end();
                            return;
                        }
                        vertx.deployVerticle("src/query-builder/dist/cubitt-query-builder.js", options, function (res, res_err) {
                            if (res_err) {
                                console.log("Query handler deployment failed! - " + res_err);
                                response.setStatusCode(500).end();
                            }
                        });
                        vertx.deployVerticle("src/command-handler/dist/cubitt-command-manager.js", options, function (res, res_err) {
                            if (res_err === null) {
                                response.putHeader("Location", "/projects/" + id.toString());
                                response.setStatusCode(204).end();
                            }
                            else {
                                console.log("Deployment failed! : " + res_err);
                                response.setStatusCode(500).end();
                            }
                        });
                    });
                });
            });
        });
    });
});
router.post("/projects/:projectid").handler(function (routingContext) {
    var transaction = routingContext.getBodyAsJson();
    var response = routingContext.response();
    response.putHeader("content-type", "application/json");
    var projectId = Common.Guid.parse(routingContext.request().getParam("projectid"));
    if (projectId === null) {
        response.setStatusCode(404).end();
        return;
    }
    sd.getClusterWideMap("actors", function (res, res_err) {
        if (res_err !== null) {
            console.log("Failed to obtain actor map: " + res_err);
            response.setStatusCode(500).end();
            return;
        }
        var actors = res;
        actors.get(projectId.toString(), function (res, res_err) {
            if (res_err !== null) {
                console.log("Could not get data from cluster wide map " + res_err);
                response.setStatusCode(500).end();
                return;
            }
            if (res === null) {
                console.log("No actor with id " + projectId + " is known");
                response.setStatusCode(404).end();
                return;
            }
            if (transaction.commands === undefined || transaction.commands === null) {
                response.setStatusCode(400).end(JSON.stringify({ status: 400, data: null, error: "Missing required commands" }));
                return;
            }
            if ((transaction.commands instanceof Array) === false || transaction.commands.length === 0) {
                response.setStatusCode(400).end(JSON.stringify({ status: 400, data: null, error: "commands should be a non-empty Array" }));
                return;
            }
            eb.send("projects.commands." + projectId.toString(), JSON.stringify(routingContext.getBodyAsJson()), function (reply, reply_err) {
                if (reply_err === null) {
                    var body = JSON.parse(reply.body());
                    response.setStatusCode(body.status).end(reply.body());
                }
                else {
                    response.setStatusCode(500).end();
                }
            });
        });
    });
});
router.get("/projects/:projectid/:version").handler(function (routingContext) {
    var response = routingContext.response();
    response.putHeader("content-type", "application/json");
    var projectId = Common.Guid.parse(routingContext.request().getParam("projectid"));
    if (projectId === null) {
        response.setStatusCode(404).end();
        return;
    }
    var version;
    if (routingContext.request().getParam("version") === "latest") {
        version = "latest";
    }
    else {
        version = parseInt(routingContext.request().getParam("version"));
    }
    eb.send("projects.query." + projectId, version, function (res, res_err) {
        if (res_err) {
            console.log("Query message delivery failure! : " + res_err);
            response.setStatusCode(500).end();
        }
        else {
            response.setStatusCode(200).end(res.body());
        }
    });
});
server.requestHandler(router.accept).listen(8080);
//# sourceMappingURL=cubitt-project-manager.js.map