jQuery.fn.dataTableExt.oSort['numeric-float-asc']  = function(a,b) {
	
	x = a.substring(a.indexOf(".") + 1, a.length)
	y = b.substring(b.indexOf(".") + 1, b.length)
	x = parseInt( x );
	y = parseInt( y );
	
	return ((x < y) ? -1 : ((x > y) ?  1 : 0));
};

jQuery.fn.dataTableExt.oSort['numeric-float-desc'] = function(a,b) {
	
	x = a.substring(a.indexOf(".") + 1, a.length)
	y = b.substring(b.indexOf(".") + 1, b.length)
	x = parseInt( x );
	y = parseInt( y );

	return ((x < y) ?  1 : ((x > y) ? -1 : 0));
};
