
let clusterDotIds = ["cc-ui","cc-json","cc-cm0","cc-cm1","cc-cm2","cc-gng","cc-gng1","cc-gng2","cc-gng3"];

function update(url, css_id) {
    if(!css_id) {
        css_id = "screenshot";
    }
    document.getElementById(css_id).src=url;
    return false;
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

function updateJob(job, url, css_id) {
    update(url, css_id);

    // Hide all the stages
    for (let i = 0; i < 100; i++) {
        let element = document.getElementById("stage-"+i);
        if (element) element.style.display = "none";
    }

    let stages = job_stages[job];
    for (let stage in stages) {
        let opts = stages[stage];
        let element = document.getElementById("stage-"+stage);
        element.style.display = "block";
        element.style.top = opts["top"];
        element.style.left = opts["left"];
        element.style.width = "300";
        element.style.height = "20";
        element.setAttribute("onclick", "window.open('spark-ui-stages.html?stage="+stage+"', '_self');");
        element.setAttribute("title", "Click to view stage "+stage);
    }
}

function updateStage() {
    if (window.location.search && window.location.search.includes("?stage=")) {
        let pos = window.location.search.indexOf("?stage=");
        let stage = window.location.search.substr(pos+7);

        hideRow();
        return update("screenshots/stages-"+stage+".png");
    }
}

function configure(left, top, width, height, last, offset, label, only, skipped) {
    if (!only) {
        only = [];
        for (let i = 0; i <= last; i++) {
            only.push(i);
        }
    }

    let row_top = top-offset;

    for (let i = 0; i <= last; i++) {
        let id = last-i;

        let element = document.getElementById("row-"+i);

        if (element) {
            if (skipped.includes(id)) {
                row_top = row_top+offset;
                element.style.display = "none";

            } else if (only.includes(id)) {
                row_top = row_top+offset;

                element.style.display = "block";
                element.style.top = row_top.toString();
                element.style.left = left;
                element.style.width = width;
                element.style.height = height;

                if (label === "jobs") {
                    element.setAttribute("onclick", "hideRow(); return updateJob("+id+", 'screenshots/"+label+"-"+id+".png');");
                } else {
                    element.setAttribute("onclick", "hideRow(); return update('screenshots/"+label+"-"+id+".png');");
                }

                let name = label;
                if (label === "sql") name = "query";
                if (label === "stages") name = "stage";
                if (label === "jobs") name = "job";
                element.setAttribute("title", "Click to view "+name+" "+id);

            } else {
                element.style.display = "none";
            }
        }
    }
}

function configureStream(left, top, width, height, offset, label, streams) {

    let row_top = top-offset;

    for (let i = 0; i < streams.length; i++) {
        let id = streams[i];
        let element = document.getElementById("row-"+i);

        if (element) {
            row_top = row_top+offset;

            element.style.display = "block";
            element.style.top = row_top.toString();
            element.style.left = left;
            element.style.width = width;
            element.style.height = height;

            if (label === "jobs") {
                element.setAttribute("onclick", "hideRow(); return updateJob("+id+", 'screenshots/"+label+"-"+id+".png');");
            } else {
                element.setAttribute("onclick", "hideRow(); return update('screenshots/"+label+"-"+id+".png');");
            }

            let name = label;
            if (label === "sql") name = "query";
            if (label === "stages") name = "stage";
            if (label === "jobs") name = "job";
            element.setAttribute("title", "Click to view "+name+" "+id);
        }
    }
}
