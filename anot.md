// Aqui criamos um canal, que vai estar rodadndo em outr thread do go
// ele estará escutando de forma assincrona as mensagens de confirmação que virá
// do kafka. Rodamos, em outra thread para que o sistema principal não fique travado
deliveryChan := make(chan kafka.Event) 


 e := <-deliveryChan //a variavel e recebe o canal que é passado na função de publicação da mensagem
 msg := e.(*kafka.Message) //msg vai receber a mensagem que o kafka nos retornou
 //Esse cara está síncrono
 if msg.TopicPartition.Error != nil {
 	fmt.Println("Erro ao enviar")
 } else {
 	fmt.Println("Mesagem enviada", msg.TopicPartition)
 }