$(function cite_query() {
    $('button#cite').on('click', function(e) {
        var text_area_value = document.getElementById("query_editor").value
        $.ajax({
            type : "POST",
            url : "cite_query",
            data : {
                query_text : text_area_value
            },
            dataType : 'html',
            // most important line! releases the document so it can be rendered
            success: function(response) {
                //document.write(response);
                var res = $('#citation_snippet', response)
                $("#citation_snippet").html(res);
            }

        });
    });
});