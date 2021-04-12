$(function execute_query() {
    $('button#execute').on('click', function(e) {
        var text_area_value = document.getElementById("query_editor").value
        $.ajax({
            type : "POST",
            url : "execute_query",
            data : {
                query_text : text_area_value // Hardcoded for now
            },
            dataType : 'html',
            // most important line! releases the document so it can be rendered
            success: function(response) {
                //document.write(response);
                var res = $('.dataframe', response)
                $("#result_set").html(res);
            }

        });
    });
});