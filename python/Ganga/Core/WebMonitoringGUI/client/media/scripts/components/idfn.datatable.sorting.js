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

jQuery.fn.dataTableExt.oSort['integer-in-tag-asc']  = function(a,b) {
	
	startIndexA = a.indexOf('>') + 1
	endIndexA = a.indexOf('</')
	x = a.substring(startIndexA, endIndexA)
	x = parseInt(x)

	startIndexB = b.indexOf('>') + 1
	endIndexB = b.indexOf('</')
	y = b.substring(startIndexB, endIndexB)
	y = parseInt(y)
	
	return ((x < y) ? -1 : ((x > y) ?  1 : 0));
};

jQuery.fn.dataTableExt.oSort['integer-in-tag-desc'] = function(a,b) {
	
	startIndexA = a.indexOf('>') + 1
	endIndexA = a.indexOf('</')
	x = a.substring(startIndexA, endIndexA)
	x = parseInt(x)

	startIndexB = b.indexOf('>') + 1
	endIndexB = b.indexOf('</')
	y = b.substring(startIndexB, endIndexB)
	y = parseInt(y)

	return ((x < y) ?  1 : ((x > y) ? -1 : 0));
};
