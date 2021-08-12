$(function cite_query() {
    $('button#mint_query_pid').on('click', function(e) {
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
                var res = $('#citation_snippet', response)
                $("#citation_snippet").html(res);
                var execution_timestamp = $('#execution_timestamp', response)
                $("#execution_timestamp").html(execution_timestamp);
                var yn_query_exists = $('#yn_query_exists', response)
                $("#yn_query_exists").html(yn_query_exists);
                var yn_result_set_changed = $('#yn_result_set_changed', response)
                $("#yn_result_set_changed").html(yn_result_set_changed);
                var yn_unique_sort_index = $('#yn_unique_sort_index', response)
                $("#yn_unique_sort_index").html(yn_unique_sort_index);

                $('.flashes', response).dialog({
                    dialogClass: 'flashes'
                })


            }

        });
    });
});