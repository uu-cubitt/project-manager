declare var require: any;
declare var process: any;
import Router = require("vertx-web-js/router");
import BodyHandler = require("vertx-web-js/body_handler");
let server = vertx.createHttpServer();
let eb = vertx.eventBus();
let router = Router.router(vertx);
import * as Common from "cubitt-common";
import JDBCClient = require("vertx-jdbc-js/jdbc_client");
let sd = vertx.sharedData();

let postgresUser = process.env.POSTGRES_USER || "postgres";
let postgresPass = process.env.POSTGRES_PASSWORD || "";
let postgresDb = process.env.POSTGRES_DB || postgresUser;
let postgresHost = process.env.POSTGRES_HOST || "localhost";
let postgresPort = process.env.POSTGRES_PORT || 5432;

let client = JDBCClient.createShared(vertx, {
	"url" : "jdbc:postgresql://" + postgresHost + ":" + postgresPort + "/" + postgresDb + "?user=" + postgresUser + "&password=" + postgresPass,
	"driver_class" : "org.postgresql.Driver",
	"max_pool_size" : 30
});

router.route().handler(BodyHandler.create().handle);

// Register route
let createRoute = router.route("POST", "/projects/").consumes("application/json").produces("application/json");

createRoute.handler(function (routingContext: RoutingContext): any {
	let body: any = routingContext.getBodyAsJson();
	let response: HttpServerResponse = routingContext.response();
	let id: Common.Guid = Common.Guid.parse(body.id);
	if (id === null) {
		response.setStatusCode(400).end(JSON.stringify({ status: 400, data: null, error: "id should be a valid GUID" }));
		return;
	}
	let options: Object = {
		"config" : {
			"id" : id
		}
	};
	// Check if we already have such an project actor
	sd.getClusterWideMap("actors", function (res, res_err) {
		if (res_err !== null) {
			console.log("Failed to obtain actor map: " + res_err);
			response.setStatusCode(500).end();
			return;
		}
		let actors = res;

		actors.putIfAbsent(id.toString(), true, function(res,res_err) {
			if (res_err !== null) {
				console.log("Failed to check/set value in actor map: " + res_err);
				response.setStatusCode(500).end();
				return;
			}
			// Key already exists, do not create a new project
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

				let connection = conn;

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
						vertx.deployVerticle("src/command-handler/dist/cubitt-command-manager.js", options, function (res: string, res_err: any): any {
							if (res_err === null) {
								response.putHeader("Location", "/projects/" + id.toString());
								response.setStatusCode(204).end();
							} else {
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

router.post("/projects/:projectid").handler(function (routingContext: RoutingContext): any {
	let transaction: any = routingContext.getBodyAsJson();
	let response: HttpServerResponse = routingContext.response();
	response.putHeader("content-type", "application/json");
	let projectId: Common.Guid = Common.Guid.parse(routingContext.request().getParam("projectid"));
	if (projectId === null) {
		response.setStatusCode(404).end();
		return;
	}
	// Check if we have such an Actor with that ID
	// Check if we already have such an project actor
	sd.getClusterWideMap("actors", function (res, res_err) {
		if (res_err !== null) {
			console.log("Failed to obtain actor map: " + res_err);
			response.setStatusCode(500).end();
			return;
		}
		let actors = res;

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
			// Validate postbody
			if (transaction.commands === undefined || transaction.commands === null) {
				response.setStatusCode(400).end(JSON.stringify({ status: 400, data: null, error: "Missing required commands" }));
				return;
			}
			if ((transaction.commands instanceof Array) === false || transaction.commands.length === 0) {
				response.setStatusCode(400).end(JSON.stringify({ status: 400, data: null, error: "commands should be a non-empty Array" }));
				return;
			}
			eb.send("projects.commands." + projectId.toString(), JSON.stringify(routingContext.getBodyAsJson()), function (reply: Message, reply_err: any): any {
				if (reply_err === null) {
					// Write to the response and end it
					let body: any = JSON.parse(reply.body());
					response.setStatusCode(body.status).end(reply.body());
				} else {
					response.setStatusCode(500).end();
				}
			});
		});
	});
});

router.get("/projects/:projectid/:version").handler(function (routingContext: RoutingContext): any {
	let response: HttpServerResponse = routingContext.response();
	response.putHeader("content-type", "application/json");
	let projectId: Common.Guid = Common.Guid.parse(routingContext.request().getParam("projectid"));
	if (projectId === null) {
		response.setStatusCode(404).end();
		return;
	}

	let version: number | string;
	if (routingContext.request().getParam("version") === "latest"){
		version = "latest";
	} else {
		version = parseInt(routingContext.request().getParam("version"));
	}

	eb.send("projects.query." + projectId, version, function(res, res_err) {
		if (res_err) {
			console.log("Query message delivery failure! : " + res_err);
			response.setStatusCode(500).end();
		} else {
			response.setStatusCode(200).end(res.body());
		}
	});
});

server.requestHandler(router.accept).listen(8080);
