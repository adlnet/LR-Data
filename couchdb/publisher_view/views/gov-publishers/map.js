function(doc) {
    var sanatize = function(str){
        str = str.trim()
        var loc = str.lastIndexOf(".");
        if(loc === (str.length -1 )){
            return str.substr(0, loc);
        }
        return str;
    }
    var publishers = ["national security agency", "consumer product safety commission", "bureau of land management, department of the interior", "national endowment for the humanities", "department of the interior", "national endowment for the arts", "department of navy", "u.s. courts", "federal judicial center", "securities and exchange commission", "u.s. mint, treasury", "department of energy", "national library of medicine", "department of commerce, international trade administration", "the white house", "national constitution center", "the federal reserve", "federal bureau of investigation", "national archives and records administration", "national science foundation", "national academy of sciences", "department of housing and urban development", "multiple agencies", "abraham lincoln bicentennial commission", "smithsonian institution", "fish and wildlife service, department of interior", "national institutes of health", "national institute of standards and technology", "library of congress", "government printing office", "general services administration", "national gallery of art", "department of justice", "department of agriculture", "institute of museum and library services", "department of homeland security", "federal trade commission", "u.s. agency for international development", "u.s. geological survey", "national oceanic and atmospheric administration", "department of health and human services", "department of the treasury", "centers for disease control and prevention", "internal revenue service", "office of naval research", "department of commerce", "u.s. institute of peace", "food and drug administration", "national park service", "u.s. global change research program", "holocaust memorial museum", "department of education", "department of state", "department of veterans affairs", "environmental protection agency", "small business administration", "federal deposit insurance corporation", "federal emergency management agency", "house of representatives", "u.s. census bureau", "department of army", "national aeronautics and space administration", "central intelligence agency", "national park service, teaching with historic places", "department of labor", "peace corps"];    
    if(doc.publisher){
        var lsPublisher =doc.publisher.toLowerCase();
        if (publishers.indexOf(lsPublisher) > -1){
            emit(lsPublisher, doc.publisher);
        }
    } 
}