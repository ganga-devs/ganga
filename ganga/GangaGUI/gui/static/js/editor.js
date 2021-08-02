  var editor = ace.edit("editor");
  editor.getSession().setUseWorker(false);
  editor.setTheme("ace/theme/monokai");
  editor.getSession().setMode("ace/mode/javascript");

  $('#mode').on('change', function (ev) {
    var mode = $('option:selected').attr('value');
    editor.getSession().setMode(mode);
  });
  
  (function($) {
    $(function() {
      document.getElementById("save").addEventListener("click", ()=>{
      var file = new File([editor.getValue()], "code.txt", {type: "text/plain;charset=utf-8"});
      saveAs(file);
    })
        
    });
})(jQuery);