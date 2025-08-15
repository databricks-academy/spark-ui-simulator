let logger = {
    NONE:  0,
    ERROR: 1,
    WARN:  2,
    INFO:  3,
    DEBUG: 4,
    level: 0,
    info: function(msg) {
        if (this.level >= this.INFO) console.log(msg);
    },
    debug: function(msg) {
        if (this.level >= this.DEBUG) console.debug(msg);
    },
    logContext(callerArgs) {
        let msg = " - "+callerArgs.callee.name+"(";
        for (let i = 0; i < callerArgs.length; i++) {
            msg += callerArgs[i];
            if (i < callerArgs.length-1) msg += ", ";
        }
        msg += ") ["+experiment+", "+version+", "+artifactType+", "+artifactId+"]";
        this.info(msg+")");
    }
}
logger.level = logger.NONE;
// logger.level = logger.INFO;
// logger.level = logger.DEBUG;

function buildQuery() {
    let query = "";
    for (let i = 0; i < arguments.length; i += 2) {
        query += (query === "" ? "?" : "&");
        query += arguments[i];
        query += "=";
        query += (i < arguments.length ? arguments[i+1] : "");
    }
    return query;
}

function standardQuery() {
    if (artifactType) {
        return buildQuery("experiment", experiment, "version", version, "artifactType", artifactType, "artifactId", artifactId);
    } else {
        return buildQuery("experiment", experiment, "version", version);
    }
}

function initExperiment(commonUiVersion) {
    logger.info(" - "+arguments.callee.name+"("+commonUiVersion+")");

    logger.debug(" --- commonUiVersion: "+commonUiVersion)

    // Expecting something like .../spark-ui-simulator/experiment-5287A/v002-S/index.html
    let path = window.location.pathname.split("/");

    let version = path[path.length-2];
    logger.debug(" --- version: "+version);

    let experiment = path[path.length-3].split("-")[1];
    logger.debug(" --- experiment: "+experiment);

    // Expecting something like job-3
    logger.debug(" --- window.location.search: " + window.location.search);
    let urlParams = new URLSearchParams(window.location.search);
    let artifact =  urlParams.has("artifact") ? urlParams.get("artifact") : null;

    let artifactParts = artifact ? artifact.split("-") : [];
    let artifactType = (artifactParts.length > 0 ? artifactParts[0] : null);

    if (artifactType === "") artifactType = null;
    logger.debug(" --- artifactType: " + artifactType);

    let artifactId = (artifactParts.length > 1 ? artifactParts.slice(1).join("-") : null);
    if (artifactId === "") artifactId = null;
    logger.debug(" --- artifactId: " + artifactId);

    let url = "../../common-ui/"+commonUiVersion+"/main.html" + buildQuery("experiment", experiment, "version", version, "artifactType", artifactType, "artifactId", artifactId);
    logger.debug(" --- url: " + url);
    document.getElementById("index-ui").src=url;
}
