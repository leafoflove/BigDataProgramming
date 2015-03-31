/*
 * jQuery File Upload Plugin JS Example 8.8.2
 * https://github.com/blueimp/jQuery-File-Upload
 *
 * Copyright 2010, Sebastian Tschan
 * https://blueimp.net
 *
 * Licensed under the MIT license:
 * http://www.opensource.org/licenses/MIT
 */

/*jslint nomen: true, regexp: true */
/*global $, window, blueimp */

$(function () {
    'use strict';
   
    $('#imageSelector a').click(function (e) {
    	e.preventDefault();
    	$(this).tab('show');
    });

    $('#imageSelector a[href="#rawImages"]').tab('show'); // Select tab by name
    $('#imageSelector a[href="#standardImage"]').tab('show'); // Select tab by name
    
    // Initialize the jQuery File Upload widget:
    $('#fileupload').fileupload({
    	acceptFileTypes: /(\.|\/)(gif|jpe?g|png|tif|tiff)$/i,
    	prependFiles: true,
    });

    // Enable iframe cross-domain access via redirect option:
    $('#fileupload').fileupload(
        'option',
        'redirect',
        window.location.href.replace(
            /\/[^\/]*$/,
            '/cors/result.html?%s'
        )
    );
    
    function getCookie(name) {
        var cookieValue = null;
        if (document.cookie && document.cookie != '') {
            var cookies = document.cookie.split(';');
            for (var i = 0; i < cookies.length; i++) {
                var cookie = jQuery.trim(cookies[i]);
                // Does this cookie string begin with the name we want?
                if (cookie.substring(0, name.length + 1) == (name + '=')) {
                    cookieValue = decodeURIComponent(cookie.substring(name.length + 1));
                    break;
                }
            }
        }
        return cookieValue;
    }
    
    $('#fileupload').bind('fileuploadsubmit', function (e, data) {
        var inputs = data.context.find(':input');
        var faults = inputs.filter(function() {
            // filter input elements to required ones that are empty
            return $(this).data('required') && $(this).val() === "";
        });
        if(faults.length) {
        	faults.first().focus();
        	faults.first().tooltip('destroy')
            	.attr('data-original-title', "Please correct the input.")
            	.tooltip('fixTitle')
            	.tooltip('show');
        	return false;
        }
        //if (inputs.filter('[required][value=""]').first().focus().length) {
        //    return false;
        //}
        var fields = inputs.serializeArray();
        // This thing took my(nanda) lifetime to figure out. Django was not simply
        // letting me set the formData without the CSRF tofen.	
        fields.push({
            name: "csrfmiddlewaretoken",
            value: getCookie('csrftoken')
        });
        data.formData = fields;
        //data.formData.append('csrfmiddlewaretoken', '{{ csrf_token }}');
    });

    // Load existing files:
    $('#fileupload').addClass('fileupload-processing');
    $.ajax({
    	// Uncomment the following to send cross-domain cookies:
    	url: './view/',
    	dataType: 'json',
    	context: $('#fileupload')[0]
    }).always(function () {
    	$(this).removeClass('fileupload-processing');
    }).done(function (result) {
    	$(this).fileupload('option', 'done')
    		.call(this, null, {result: result});
    });
    
});
