logger.info("main.js");

let clusterDotIds = ["cc-ui","cc-json","cc-cm0","cc-cm1","cc-cm2","cc-gng","cc-gng1","cc-gng2","cc-gng3"];

let urlParams = new URLSearchParams(window.location.search);
let version = urlParams.get("version");
let experiment = urlParams.get("experiment");

// Expecting something like job-3
logger.debug(" - "+window.location.href);

let artifactType =  urlParams.has("artifactType") ? urlParams.get("artifactType") : null;
if (artifactType === "null") artifactType = null;
logger.debug(" - artifactType: '" + artifactType + "' (" + typeof(artifactType) + ")");

let artifactId = urlParams.has("artifactId") ? urlParams.get("artifactId") : null;
if (artifactId === "null") artifactId = null;
logger.debug(" - artifactId: '" + artifactId + "' (" + typeof(artifactId) + ")");

function updateScreenshot(image) {
    logger.logContext(arguments);

    let url = "../../experiment-" + experiment + "/" + version + "/screenshots/" + image;
    logger.debug(" --- url: " + url);

    document.getElementById("screenshot").src=url;
}
function answerIs(self, expected) {
    if (self.value === "") {
        self.style.backgroundColor="#ffffff";
    } else if (expected.includes(self.value.toLowerCase())) {
        self.style.backgroundColor="#7ffe78";
    } else {
        self.style.backgroundColor="#ffb9bb";
    }
}
function hide(ids) {
    for (let i =0; i < ids.length; i++) {
        let id = ids[i];
        let element = document.getElementById(id);
        if (element) element.style.display = "none";
    }
}
function show(ids) {
    for (let i =0; i < ids.length; i++) {
        let id = ids[i];
        let element = document.getElementById(id);
        if (element) element.style.display = "block";
    }
}
function hideShow(hideIds, showIds) {
    hide(hideIds);
    show(showIds);
}
function hideRow() {
    for (let i = 0; i < 100; i++) {
        let element = document.getElementById("row-"+i);
        if (element) element.style.display = "none";
    }
}

function initMainUI() {
    logger.logContext(arguments);

    let script = document.createElement('script');
    script.onload = function() {
        logger.logContext(arguments);

        document.getElementById("link-notebook").setAttribute("href", "../../experiment-"+experiment+"/"+version+"/index.html?artifact=notebook");
        document.getElementById("link-spark-ui").setAttribute("href", "../../experiment-"+experiment+"/"+version+"/index.html?artifact=jobs");
        document.getElementById("link-cluster").setAttribute("href",  "../../experiment-"+experiment+"/"+version+"/index.html?artifact=cluster_conf");

        document.getElementById("link-src-scala").setAttribute("href", "../../experiment-"+experiment+"/"+version+"/source-code.scala");
        if (experiment_conf.language === "python") document.getElementById("link-src-scala").style.display = "none";

        document.getElementById("link-src-python").setAttribute("href", "../../experiment-"+experiment+"/"+version+"/source-code.py");
        if (experiment_conf.language === "scala") document.getElementById("link-src-python").style.display = "none";

        let has_lab = (experiment_conf.has_lab === "true" || experiment_conf.has_lab === true);
        document.getElementById("link-lab").style.display = has_lab ? "inline-block" : "none";
        document.getElementById("link-lab").setAttribute("href", "../../experiment-"+experiment+"/"+version+"/lab.html");

        document.getElementById("main_title").innerText = "The Spark UI Simulator - Experiment #" + experiment;

        if (["jobs", "stages", "storage", "environment", "executors", "sql", "jdbc", "streaming"].includes(artifactType)) {
            let url = "spark-ui.html" + standardQuery();
            logger.debug(" --- url: " + url);
            window.open(url, "main-ui");

        } else if (["cluster_conf", "cluster_json", "cluster_notebooks", "cluster_libraries", "cluster_events", "cluster_driver_logs", "cluster_metrics", "cluster_apps", "cluster_master", "cluster_master_0", "cluster_master_1", "cluster_master_2"].includes(artifactType)) {
            let url = "cluster.html" + standardQuery();
            logger.debug(" --- url: " + url);
            window.open(url, "main-ui");

        } else if (artifactType === null || artifactType === 'notebook') {
            let url = "notebook.html" + standardQuery();
            logger.debug(" --- url: " + url);
            window.open(url, "main-ui");

        } else {
            alert("Unknown artifactType in initMainUI(): " + artifactType);
        }
    };
    script.src = "../../experiment-"+experiment+"/"+version+"/_config/experiment.js";
    document.head.appendChild(script);
}

function selectPage(page, target) {
    logger.logContext(arguments);
    let url = page + standardQuery();
    logger.debug(" - url: " + url);
    document.getElementById(target).src=url;
}

function initNotebook() {
    logger.logContext(arguments);

    let url = "../../experiment-"+experiment+"/"+version+"/screenshots/notebook.png";
    document.getElementById("notebook-png").src=url;
    logger.debug(" - url: " + url);
}

function setDeepLink(target, name, id) {
    logger.logContext(arguments);

    let url = "../../experiment-"+experiment+"/"+version+"/index.html?artifact="+name;
    if (id) url += ("-"+id);

    logger.debug(" --- url: " + url);

    document.getElementById(target).setAttribute("href",    url);
}

function initSparkUI() {
    logger.logContext(arguments);

    setDeepLink("link-sui-jobs", "jobs");
    setDeepLink("link-sui-stages", "stages");
    setDeepLink("link-sui-storage", "storage");
    setDeepLink("link-sui-env", "environment");
    setDeepLink("link-sui-exec", "executors");
    setDeepLink("link-sui-sql", "sql");
    setDeepLink("link-sui-jdbc", "jdbc");
    setDeepLink("link-sui-streams", "streaming");

    if (artifactType === null || ["jobs", "stages", "storage", "environment", "executors", "sql", "jdbc", "streaming"].includes(artifactType)) {
        selectPage("spark-ui-"+artifactType+".html", "spark-ui");

    } else {
        alert("Unknown artifactType in initSparkUI(): " + artifactType);
    }
}

function initJobs() {
    logger.logContext(arguments);
    let scriptA = document.createElement('script');
    scriptA.onload = initJobsA;
    scriptA.src = "../../experiment-"+experiment+"/"+version+"/_config/jobs-config.js";
    document.head.appendChild(scriptA);
}
function initJobsA() {
    logger.logContext(arguments);
    let scriptB = document.createElement('script');
    scriptB.onload = initJobsB;
    scriptB.src = "../../experiment-"+experiment+"/"+version+"/_config/job_stages.js";
    document.head.appendChild(scriptB);
}
function initJobsB() {
    logger.logContext(arguments);

    if (typeof jobs_locations === 'undefined') {
        // Supporting backwards compatibility with version 2 experiments
        configure(jobs_left, jobs_top, jobs_width, jobs_height, jobs_last, jobs_offset, 'jobs', jobs_only, jobs_skipped, {});
    } else {
        configure(jobs_left, jobs_top, jobs_width, jobs_height, jobs_last, jobs_offset, 'jobs', jobs_only, jobs_skipped, jobs_locations);
    }

    if (artifactId) {
        showDetailPage("jobs");
    } else {
        document.getElementById("screenshot").src="../../experiment-"+experiment+"/"+version+"/screenshots/jobs.png";
    }

    // Hide all the stages
    for (let i = 0; i < 100; i++) {
        let element = document.getElementById("stage-"+i);
        if (element) element.style.display = "none";
    }

    let stages = job_stages[artifactId];
    for (let stage in stages) {
        let opts = stages[stage];
        let element = document.getElementById("stage-"+stage);
        element.style.display = "block";
        element.style.top = opts["top"];
        element.style.left = opts["left"];
        element.style.width = "300";
        element.style.height = "20";

        let url = "../../experiment-"+experiment+"/"+version+"/index.html?artifact=stages-"+stage;
        element.setAttribute("href", url);
        element.setAttribute("title", "Click to view stage "+stage);
    }
}

function initStages() {
    logger.logContext(arguments);
    let scriptA = document.createElement('script');
    scriptA.onload = function() {
        logger.logContext(arguments);

        if (typeof stages_locations === 'undefined') {
            // Supporting backwards compatibility with version 2 experiments
            configure(stages_left, stages_top, stages_width, stages_height, stages_last, stages_offset, 'stages', stages_only, stages_skipped, {});
        } else {
            configure(stages_left, stages_top, stages_width, stages_height, stages_last, stages_offset, 'stages', stages_only, stages_skipped, stages_locations);
        }

        if (artifactId) {
            showDetailPage("stages");
        } else {
            document.getElementById("screenshot").src="../../experiment-"+experiment+"/"+version+"/screenshots/stages.png";
        }
    };
    scriptA.src = "../../experiment-"+experiment+"/"+version+"/_config/stages-config.js";
    document.head.appendChild(scriptA);
}

function initStorage() {
    logger.logContext(arguments);
    let scriptA = document.createElement('script');
    scriptA.onload = function() {
        logger.logContext(arguments);

        if (typeof storage_locations === 'undefined') {
            configure(storage_left, storage_top, storage_width, storage_height, storage_last, storage_offset, 'storage', storage_only, storage_skipped, {});
        } else {
            configure(storage_left, storage_top, storage_width, storage_height, storage_last, storage_offset, 'storage', storage_only, storage_skipped, storage_locations);
        }

        if (artifactId) {
            showDetailPage("storage");
        } else {
            document.getElementById("screenshot").src="../../experiment-"+experiment+"/"+version+"/screenshots/storage.png";
        }
    };
    scriptA.src = "../../experiment-"+experiment+"/"+version+"/_config/storage-config.js";
    document.head.appendChild(scriptA);
}

function initEnvironment() {
    logger.logContext(arguments);
    document.getElementById("screenshot").src="../../experiment-"+experiment+"/"+version+"/screenshots/environment.png";
}

function initExecutors() {
    logger.logContext(arguments);
    document.getElementById("screenshot").src="../../experiment-"+experiment+"/"+version+"/screenshots/executors.png";
}

function initSql() {
    logger.logContext(arguments);
    let scriptA = document.createElement('script');
    scriptA.onload = function() {
        logger.logContext(arguments);

        if (typeof sql_locations === "undefined") {
            configure(sql_left, sql_top, sql_width, sql_height, sql_last, sql_offset, 'sql', sql_only, sql_skipped, {});
        } else {
            configure(sql_left, sql_top, sql_width, sql_height, sql_last, sql_offset, 'sql', sql_only, sql_skipped, sql_locations);
        }

        if (artifactId) {
            showDetailPage("sql");
        } else {
            document.getElementById("screenshot").src="../../experiment-"+experiment+"/"+version+"/screenshots/sql.png";
        }
    };
    scriptA.src = "../../experiment-"+experiment+"/"+version+"/_config/sql-config.js";
    document.head.appendChild(scriptA);
}

function initJDBC() {
    logger.logContext(arguments);
    document.getElementById("screenshot").src="../../common-screenshots/jdbc.png";
}

function initStreaming() {
    logger.logContext(arguments);
    let scriptA = document.createElement('script');
    scriptA.onload = function() {
        logger.logContext(arguments);

        if (typeof stream_locations === "undefined") {
            configureStream(stream_left, stream_top, stream_width, stream_height, stream_offset, 'stream', stream_only, {})
        } else {
            configureStream(stream_left, stream_top, stream_width, stream_height, stream_offset, 'stream', stream_only, stream_locations)
        }

        if (artifactId) {
            showDetailPage("streaming");
        } else {
            document.getElementById("screenshot").src="../../experiment-"+experiment+"/"+version+"/screenshots/streaming.png";
        }
    };
    scriptA.src = "../../experiment-"+experiment+"/"+version+"/_config/stream-config.js";
    document.head.appendChild(scriptA);
}

function initCluster() {
    logger.logContext(arguments);

    // Hide these always, at least to start with
    hide(['cc-ui', 'cc-cm0','cc-cm1','cc-cm2','cc-gng',
        'cc-gng0','cc-gng1','cc-gng2','cc-gng3','cc-gng4','cc-gng5','cc-gng6','cc-gng7','cc-gng8','cc-gng9'
    ]);

    setDeepLink("cc-conf", "cluster_conf");
    setDeepLink("cc-ui", "cluster_conf");
    setDeepLink("cc-json", "cluster_json");

    setDeepLink("cc-note", "cluster_notebooks");
    setDeepLink("cc-lib", "cluster_libraries");
    setDeepLink("cc-evt", "cluster_events");
    setDeepLink("cc-sui", "jobs");
    setDeepLink("cc-log", "cluster_driver_logs");

    setDeepLink("cc-met", "cluster_metrics");

    document.getElementById("cc-gng1").setAttribute("href",    "../../experiment-"+experiment+"/"+version+"/screenshots/ganglia-1.png");
    document.getElementById("cc-gng2").setAttribute("href",    "../../experiment-"+experiment+"/"+version+"/screenshots/ganglia-2.png");
    document.getElementById("cc-gng3").setAttribute("href",    "../../experiment-"+experiment+"/"+version+"/screenshots/ganglia-3.png");

    setDeepLink("cc-app", "cluster_apps");

    setDeepLink("cc-mas", "cluster_master");
    setDeepLink("cc-cm0", "cluster_master_0");
    setDeepLink("cc-cm1", "cluster_master_1");
    setDeepLink("cc-cm2", "cluster_master_2");

    if (artifactType === "cluster_conf") {
        hideShow(clusterDotIds, ['cc-json']);
        updateScreenshot("cluster.png");

    } else if (artifactType === "cluster_json") {
        hideShow(clusterDotIds, ['cc-ui'])
        updateScreenshot("cluster-json.png");

    } else if (artifactType === "cluster_notebooks") {
        hideShow(clusterDotIds,[]);
        updateScreenshot("cluster-notebooks.png");

    } else if (artifactType === "cluster_libraries") {
        hideShow(clusterDotIds,[]);
        updateScreenshot("cluster-libraries.png")

    } else if (artifactType === "cluster_events") {
        hideShow(clusterDotIds,[]);
        updateScreenshot("cluster-event-log.png")

    } else if (artifactType === "cluster_events") {
        hideShow(clusterDotIds, []);
        updateScreenshot("cluster-event-log.png")

    } else if (artifactType === "cluster_driver_logs") {
        hideShow(clusterDotIds,[]);
        updateScreenshot("cluster-driver-logs.png");

    } else if (artifactType === "cluster_metrics") {
        hideShow(clusterDotIds,['cc-gng','cc-gng1','cc-gng2','cc-gng3']);
        updateScreenshot("cluster-metrics.png");

    } else if (artifactType === "cluster_apps") {
        hideShow(clusterDotIds,[]);
        updateScreenshot("cluster-apps.png");

    } else if (artifactType === "cluster_apps") {
        hideShow(clusterDotIds,[]);
        updateScreenshot("cluster-apps.png");

    } else if (artifactType === "cluster_master") {
        hideShow(clusterDotIds,['cc-cm0','cc-cm1','cc-cm2']);
        updateScreenshot("cluster-master.png");

    } else if (artifactType === "cluster_master_0") {
        hideShow(clusterDotIds,['cc-cm0','cc-cm1','cc-cm2']);
        updateScreenshot("cluster-master.png");

    } else if (artifactType === "cluster_master_1") {
        hideShow(clusterDotIds,['cc-cm0','cc-cm1','cc-cm2']);
        updateScreenshot("cluster-master-0.png");

    } else if (artifactType === "cluster_master_2") {
        hideShow(clusterDotIds,['cc-cm0','cc-cm1','cc-cm2']);
        updateScreenshot("cluster-master-1.png");

    } else if (artifactType === null || artifactType === "cluster_config") {
        hideShow(clusterDotIds,[]);
        updateScreenshot("cluster.png");

    } else {
        alert("Unknown artifactType in initCluster(): " + artifactType);
    }
}

function showDetailPage(prefix) {
    logger.logContext(arguments);
    if (artifactId) {
        hideRow();
        updateScreenshot(prefix+"-"+artifactId+".png");
    }
}

function configure(left, top, width, height, last, offset, label, only, skipped, locations) {
    logger.logContext(arguments);

    if (!only) {
        only = [];
        for (let i = 0; i <= last; i++) {
            only.push(i);
        }
    }

    if (Object.keys(locations).length > 0) {
        logger.debug(" --- Using new-school positioning");
    } else {
        logger.debug(" --- Using old-school positioning");
    }

    let row_top = top-offset;

    for (let i = 0; i <= last; i++) {
        let id = last-i;

        let element = document.getElementById("row-"+i);

        if (element) {
            if (skipped.includes(id)) {
                row_top = row_top+offset;
                element.style.display = "none";
                // logger.debug(" - Skipping Artifact: " + id);

            } else if (only.includes(id)) {
                row_top = row_top+offset;

                // logger.debug("Processing row " + i + ", id " + id + " @ " + row_top);
                element.style.display = "block";

                if (locations[id]) {
                    let hOffset = 10;
                    let vOffset = 4;
                    element.style.left = locations[id]["x"] - hOffset;
                    element.style.top = locations[id]["y"] - vOffset;
                    element.style.width = locations[id]["w"] + hOffset;
                    element.style.height = locations[id]["h"] + vOffset;

                } else {
                    element.style.top = row_top.toString();
                    element.style.left = left;
                    element.style.width = width;
                    element.style.height = height;
                }

                let url = "../../experiment-"+experiment+"/"+version+"/index.html?artifact="+artifactType+"-"+id;
                element.setAttribute("href", url);

                let name = label;
                if (label === "sql") name = "query";
                if (label === "stages") name = "stage";
                if (label === "jobs") name = "job";

                element.setAttribute("title", "Click to view "+name+" "+id);

            } else {
                element.style.display = "none";
                // logger.debug(" - Artifact Not Found: " + id);
            }
        } else {
            // logger.debug("Not found " + i + ", id " + id);
        }
    }
}

function configureStream(left, top, width, height, offset, label, streams, locations) {
    logger.logContext(arguments);

    let row_top = top-offset;

    if (Object.keys(locations).length > 0) {
        logger.debug(" --- Using new-school positioning");
    } else {
        logger.debug(" --- Using old-school positioning");
    }

    for (let i = 0; i < streams.length; i++) {
        let id = streams[i];
        let element = document.getElementById("row-"+i);

        if (element) {
            row_top = row_top+offset;

            element.style.display = "block";
            element.style.top = row_top.toString();

            if (locations[id]) {
                let hOffset = 10;
                let vOffset = 4;
                element.style.left = locations[id]["x"] - hOffset;
                element.style.top = locations[id]["y"] - vOffset;
                element.style.width = locations[id]["w"] + hOffset;
                element.style.height = locations[id]["h"] + vOffset;

            } else {
                element.style.top = row_top.toString();
                element.style.left = left;
                element.style.width = width;
                element.style.height = height;
            }

            element.setAttribute("onclick", "hideRow(); return update('screenshots/"+label+"-"+id+".png');");

            let url = "../../experiment-"+experiment+"/"+version+"/index.html?artifact=streaming-"+id;
            element.setAttribute("href", url);

            element.setAttribute("title", "Click to view "+label+" "+id);
        } else {
            // logger.info("Not found " + i + ", id " + id);
        }
    }
}
