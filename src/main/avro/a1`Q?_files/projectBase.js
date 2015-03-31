$(function () {
    
	$('#edit_cover_file').change(function(){
		var file = this.files[0];
		
		var formData = new FormData();
		formData.append('cover_pic', file, file.name);
		formData.append("csrfmiddlewaretoken", getCookie('csrftoken'));

		var xhr = new XMLHttpRequest();
		xhr.open('POST', 'updateProjectCover', 'true');
		
		// Set up a handler for when the request finishes.
		xhr.onload = function () {
			if (xhr.status === 200) {
		      // File(s) uploaded.
				var url = JSON.parse(xhr.responseText)["url"];
				document.getElementById("cover_pic").src=url;
			} else {
				alert('Could not edit cover picture !!');
			}
			document.getElementById("edit_cover_span").textContent="Edit Cover";
		};
		document.getElementById("edit_cover_span").textContent="Editing...";
		xhr.send(formData);
	});

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

	function csrfSafeMethod(method) {
	    // these HTTP methods do not require CSRF protection
	    return (/^(GET|HEAD|OPTIONS|TRACE)$/.test(method));
	}
	
	var csrftoken = getCookie('csrftoken');
	$.ajaxSetup({
	    crossDomain: false, // obviates need for sameOrigin test
	    beforeSend: function(xhr, settings) {
	        if (!csrfSafeMethod(settings.type)) {
	            xhr.setRequestHeader("X-CSRFToken", csrftoken);
	        }
	    }
	});

	$("#addCollaboratorsForm").submit(function(event) {
		event.preventDefault();
		$.post( "addCollaborator/", { user: $('#collaboratorsText').val()})
		 .done(function(data) {
			 $("#collaboratorAddResult").html(data.message);
			 $("#collaboratorAddResult").addClass("alert");
			 if (data.success == false) {
				 $("#collaboratorAddResult").addClass("alert-danger");
			 } else {
				 $("#collaboratorAddResult").addClass("alert-success");
			 }
			 // Reset text in the textbox.
			 $("#collaboratorsText").val("");
		});
	});	
	
});