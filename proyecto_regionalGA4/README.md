para google analycs universal la request debe enviarse de la siguiente manera
{
    "type_extraction": "Yesterday"
    "init_date":
    "end_date":
}
recibe las opciones Last7Days, Yesterday o Custom
cuando la opcion es Custom que se habilita con el proposito de obtener data historica se deben enviar dos parametros adicionales
init_date y end_date en formato "AAAA-MM-DD", si elige las opciones Yesterday o Last7Days puede enviar los valores vacios ""

para google analytics GA4 debe enviarse la request de la siguiente forma

