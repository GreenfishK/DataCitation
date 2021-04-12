$(function execute_query() {
    $('button#execute').on('click', function(e) {
        var text_area_value = document.getElementById("query_editor").value
        $.ajax({
            type : "POST",
            url : "execute_query",
            data : {
                query_text : text_area_value // Hardcoded for now
            },
            success : function(data){
                console.log("success");
            }
        });
    });
});