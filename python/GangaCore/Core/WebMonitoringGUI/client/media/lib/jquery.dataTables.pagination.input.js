/*
URL Pagination plugin for jQuery.dataTables
by Lukasz Kokoszkiewicz
ver. 1.0.0
tested with Data Tables v1.6.2

How to use
==========
Initialisation:
$(document).ready(function() {
    $('#example').dataTable( {
        "sPaginationType": "input"
    } );
} );

To your *.html file add line:
<input type="hidden" id="url-page" />

And when feel nessesary trigger event click on above element:
$('#url-page').trigger('click');
The page will be set from URL variable 'p' (eg. <url>?p=3)
If you want to load variable from url hash just add `value="hash"` to the `input` element
*/

$.fn.dataTableExt.oPagination.input = {
        /*
         * Function: oPagination.input.fnInit
         * Purpose:  Initalise dom elements required for pagination with input textbox
         * Returns:  -
         * Inputs:   object:oSettings - dataTables settings object
         *           function:fnCallbackDraw - draw function which must be called on update
         */
        "fnInit": function ( oSettings, nPaging, fnCallbackDraw )
        {
                var nFirst = document.createElement( 'span' );
                var nPrevious = document.createElement( 'span' );
                var nNext = document.createElement( 'span' );
                var nLast = document.createElement( 'span' );
                var nInput = document.createElement( 'input' );
                var nPage = document.createElement( 'span' );
                var nOf = document.createElement( 'span' );
               
                nFirst.innerHTML = oSettings.oLanguage.oPaginate.sFirst;
                nPrevious.innerHTML = oSettings.oLanguage.oPaginate.sPrevious;
                nNext.innerHTML = oSettings.oLanguage.oPaginate.sNext;
                nLast.innerHTML = oSettings.oLanguage.oPaginate.sLast;
               
                nFirst.className = "paginate_button first";
                nPrevious.className = "paginate_button previous";
                nNext.className="paginate_button next";
                nLast.className = "paginate_button last";
                nOf.className = "paginate_of";
                nPage.className = "paginate_page";
               
                if ( oSettings.sTableId !== '' )
                {
                        nPaging.setAttribute( 'id', oSettings.sTableId+'_paginate' );
                        nPrevious.setAttribute( 'id', oSettings.sTableId+'_previous' );
                        nPrevious.setAttribute( 'id', oSettings.sTableId+'_previous' );
                        nNext.setAttribute( 'id', oSettings.sTableId+'_next' );
                        nLast.setAttribute( 'id', oSettings.sTableId+'_last' );
                        nFirst.setAttribute( 'id', oSettings.sTableId+'_first' );
                }
               
                nInput.type = "text";
                nInput.style.width = "15px";
                nInput.style.display = "inline";
                nPage.innerHTML = "Page ";
               
                nPaging.appendChild( nFirst );
                nPaging.appendChild( nPrevious );
                nPaging.appendChild( nPage );
                nPaging.appendChild( nInput );
                nPaging.appendChild( nOf );
                nPaging.appendChild( nNext );
                nPaging.appendChild( nLast );
               
                $(nFirst).click( function () {
                        oSettings._iDisplayStart = 0;
                        fnCallbackDraw( oSettings );
                } );
               
                $(nPrevious).click( function() {
                        oSettings._iDisplayStart -= oSettings._iDisplayLength;
                       
                        /* Correct for underrun */
                        if ( oSettings._iDisplayStart < 0 )
                        {
                          oSettings._iDisplayStart = 0;
                        }
                       
                        fnCallbackDraw( oSettings );
                } );
               
                $(nNext).click( function() {
                        /* Make sure we are not over running the display array */
                        if ( oSettings._iDisplayStart + oSettings._iDisplayLength < oSettings.fnRecordsDisplay() )
                        {
                                oSettings._iDisplayStart += oSettings._iDisplayLength;
                        }
                       
                        fnCallbackDraw( oSettings );
                } );
               
                $(nLast).click( function() {
                        var iPages = parseInt( (oSettings.fnRecordsDisplay()-1) / oSettings._iDisplayLength, 10 ) + 1;
                        oSettings._iDisplayStart = (iPages-1) * oSettings._iDisplayLength;
                       
                        fnCallbackDraw( oSettings );
                } );
               
                $(nInput).keyup( function (e) {
                       
                        if ( e.which == 38 || e.which == 39 )
                        {
                                this.value++;
                        }
                        else if ( (e.which == 37 || e.which == 40) && this.value > 1 )
                        {
                                this.value--;
                        }
                       
                        if ( this.value == "" || this.value.match(/[^0-9]/) )
                        {
                                /* Nothing entered or non-numeric character */
                                return;
                        }
                       
                        var iNewStart = oSettings._iDisplayLength * (this.value - 1);
                        if ( iNewStart > oSettings.fnRecordsDisplay() )
                        {
                                /* Display overrun */
                                oSettings._iDisplayStart = (Math.ceil((oSettings.fnRecordsDisplay()-1) /
                                        oSettings._iDisplayLength)-1) * oSettings._iDisplayLength;
                                fnCallbackDraw( oSettings );
                                return;
                        }
                       
                        oSettings._iDisplayStart = iNewStart;
                        fnCallbackDraw( oSettings );
                } );
               
                $('#url-page').click( function (e) {
                        /* Get page number from URL - BEGIN */
                        if (this.value == 'hash') var s = window.location.hash.substring(1).split('&');
                        else var s = window.location.search.substring(1).split('&');
            var c = {};
            for (var i  = 0; i < s.length; i++) {
                var parts = new Array();
                parts[0] = s[i].split('=', 1);
                parts[1] = s[i].replace(parts[0]+'=', '');
                c[unescape(parts[0])] = unescape(parts[1]);
            }
            var page = parseInt(c['p']);
                        /* Get page number from URL - END */
            if (page > 0) {
                            var iNewStart = oSettings._iDisplayLength * (page - 1);
                            if ( iNewStart > oSettings.fnRecordsDisplay() )
                            {
                                    /* Display overrun */
                                    oSettings._iDisplayStart = (Math.ceil((oSettings.fnRecordsDisplay()-1) /
                                            oSettings._iDisplayLength)-1) * oSettings._iDisplayLength;
                                    fnCallbackDraw( oSettings );
                                    return;
                            }
                       
                            oSettings._iDisplayStart = iNewStart;
                            fnCallbackDraw( oSettings );
                        }
                } );
               
                /* Take the brutal approach to cancelling text selection */
                $('span', nPaging).bind( 'mousedown', function () { return false; } );
                $('span', nPaging).bind( 'selectstart', function () { return false; } );
               
                oSettings.nPagingOf = nOf;
                oSettings.nPagingInput = nInput;
        },
       
        /*
         * Function: oPagination.input.fnUpdate
         * Purpose:  Update the input element
         * Returns:  -
         * Inputs:   object:oSettings - dataTables settings object
         *           function:fnCallbackDraw - draw function which must be called on update
         */
        "fnUpdate": function ( oSettings, fnCallbackDraw )
        {
                if ( !oSettings.aanFeatures.p )
                {
                        return;
                }
               
                var iPages = Math.ceil((oSettings.fnRecordsDisplay()) / oSettings._iDisplayLength);
                var iCurrentPage = Math.ceil(oSettings._iDisplayStart / oSettings._iDisplayLength) + 1;
               
                oSettings.nPagingOf.innerHTML = " of "+iPages
                oSettings.nPagingInput.value = iCurrentPage;
        }
}
