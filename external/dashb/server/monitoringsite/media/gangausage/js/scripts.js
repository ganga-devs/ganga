$(document).ready(function() {
	//Action
	$('.single_content').hide(); //Скриваме всички елементи които имат клас "single_content"
	$('.navi li:first').addClass('active');//добавяме клас "active" на първият елемент "li", който се намира в елемент с клас "navi"
	$('.single_content:first').show();//показваме първият елемент с клас "single_content"
	
	//On Click Event
	$('.navi li').click(function() {
		$('.navi li').removeClass('active');//премахва клас "active" на всички елементи "li", който се намират в елемент с клас "navi"
		$(this).addClass('active');//добавя клас "active" елемента активирания елемент
		$('.single_content').hide();//Скрива всички елементи които имат клас "single_content"
		var activeTab = $(this).find('a').attr('href');
		$(activeTab).fadeIn(500);//показва елемента с ефект "fadeIn" за 500 мили секунди
	});
});