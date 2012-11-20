// This file is part of the jTaskMonitoring software
// Copyright (c) CERN 2010
//
// Author: Lukasz Kokoszkiewicz [lukasz@kokoszkiewicz.com , lukasz.kokoszkiewicz@cern.ch]
//
// Requirements:
// - jquery.livesearch.js
// - quicksilver.js
//
// History:
// 18.05.2010 Created
//

(function($) {
    $.fn.lkfw_searchableList = function(settings) {
        var _config = {
            'listId': 'srchList',
            'items': [],
            'srchFldLbl': ''
        };
        
        var _buildList = function(elCnt) {
            var sFieldDiv = $('<div></div>').addClass('srchField_div');
            var sFieldForm = $('<form></form>').attr('method','get');
            var sFieldInput = $('<input />').attr({
                type: 'text',
                value: '',
                name: 'srchField_'+elCnt,
                id: 'srchField_'+elCnt
            });
            
            sFieldDiv.append(sFieldForm.append(_config.srchFldLbl).append(sFieldInput));
            
            var sList = $('<ul></ul>').attr('id', _config.listId+'_'+elCnt);
            
            var sLi;
            for (key in _config.items) {
                sLi = $('<li></li>').text(_config.items[key]);
                sList.append(sLi);
            }
            
            var output = sFieldDiv.after(sList);
            
            return output;
        }
        
        /*var _buildList = function(elCnt) {
            var list = '';
		    list += '<div class="srchField_div">';
            list += '<form method="get">';
			list += _config.srchFldLbl+'<input type="text" value="" name="srchField_'+elCnt+'" id="srchField_'+elCnt+'" />';
            list += '</form>';
			list += '</div>';
            list += '<ul id="'+_config.listId+'_'+elCnt+'">';
            
            for (key in _config.items) {
                list += '<li>'+_config.items[key]+'</li>';
            }
            
            list += '</ul>';
            
            return list;
        }*/
 
        if (settings) $.extend(_config, settings);
        
        var elCnt = 0;
        this.each(function() {
            // element-specific code here
            $(this).empty().append(_buildList(elCnt));
            $('#srchField_'+elCnt).liveUpdate(_config.listId+'_'+elCnt);
            elCnt++;
        });

        return this;
    };
})(jQuery);
