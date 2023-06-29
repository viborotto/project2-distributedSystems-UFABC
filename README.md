# project2-distributedSystems-UFABC

## 1. Definição do Sistema

Crie um sistema distribuído com três servidores que permita armazenar pares chave-valor (também denominado Sistema KV ou Key-Value Store) de forma replicada e consistente, utilizando TCP como protocolo da camada de transporte.

O sistema funcionará de forma similar (porém muito simplificada) ao sistema Zookepeer, um sistema distribuído que permite a coordenação de servidores.


## 2. Visão geral do sistema

O sistema será composto por 3 servidores (com IPs e portas conhecidas) e muitos clientes. Os clientes poderão realizar requisições em qualquer servidor, tanto para inserir informações key-value no sistema (i.e., PUT) quanto para obtê-las (i.e., GET). Os servidores, por sua vez, deverão atender as requisições dos clientes. Dentre os três servidores, escolhe-se inicialmente um deles como líder, o qual será o único que poderá realizar um PUT. Por outro lado, qualquer servidor poderá responder o GET.

## 3. Funcionalidades do Cliente (considere o ponto de vista de um ClienteX)

(a) **Inicialização**: o cliente deve capturar do teclado os IP e portas dos três servidores. As portas default dos servidores (como mencionado na Seção 5) são 10097, 10098 e 10099. Cabe destacar que o cliente não conhece (e não deve conhecer) quem é o líder.

(b) **Envio do PUT**: o cliente deve capturar do teclado a key e value a ser inserida. A seguir, envia uma requisição PUT a um servidor escolhido de forma aleatória. Deve receber a mensagem PUT_OK com um timestamp1. Para detalhes do timestamp, ver Seção 5.
* Na requisição do PUT, envie a key e a value.

(c) **Envio do GET**: o cliente deve capturar do teclado a key a ser procurada. A seguir, envia uma requisição GET a um servidor escolhido de forma aleatória. Deve receber uma resposta do servidor escolhido. Para detalhes da resposta, ver Seção 5.
* Na requisição do GET, envie a key e o último timestamp que o cliente tem associado a essa key. Note que o timestamp não deve ser capturado do teclado.

__Observações:__
* Cada cliente possui seu(s) próprio(s) timestamp(s), inicializado(s) em zero.
* Toda comunicação entre cliente <-> servidor será por TCP e deverá obrigatoriamente transferir a classe Mensagem criada por você.
* Considere que o cliente não vai sair do sistema nem morrer.


## 3. Funcionalidades do Servidor (considere o ponto de vista de um Servidor S)

(a) **Inicialização**: o servidor deve capturar do teclado o IP e a porta dele e o IP e a porta do líder. O endereço IP a ser inserido será o 127.0.0.1 se estiver realizando o projeto na mesma máquina. As portas default (que permitirão aos clientes conectar-se com algum servidor) serão as `10097`, `10098` e `10099`.


(b) **Recebe e responde** `simultaneamente (obrigatório com threads)` requisições dos clientes. Por ‘simultaneamente’ entenda-se que o servidor deverá poder realizar outras funcionalidades enquanto está fazendo requisições ou esperando por elas.


(c) **Recebe Requisição PUT**. Caso o servidor não seja o líder, deverá encaminhar a requisição para o líder. Caso o servidor seja o líder:
  1. Insere a informação em uma tabela de hash local, associando um timestamp para essa key.
       1. • Se a key já existir, atualize tanto o value quanto o timestamp associado.
  2. Replique a informação (key, value, timestamp) nos outros servidores, enviando-a na mensagem REPLICATION.
  3. Envie para o cliente a mensagem PUT_OK junto com o timestamp associado à key (ver regra de envio no item: Requisição REPLICATION_OK).


(d) **Recebe Requisição REPLICATION**. Insira na sua tabela de hash local as informações e responda para o líder a mensagem REPLICATION_OK.

(e) **Recebe Requisição REPLICATION_OK**. Assim que o líder receber essa mensagem de todos os servidores, envie para o cliente a mensagem PUT_OK junto com o timestamp associado à key.

(f) **Recebe Requisição GET**. Considere o cliente Cx com seu timestamp Tx, requisitando pela key Kx ao servidor S. Caso a chave não exista, o value devolvido será NULL. Caso exista, o value a ser devolvido para Cx será aquele cujo timestamp associado a Kx (no Servidor S) seja igual ou maior ao Tx. Se o timestamp da key em S for menor que Tx, devolva para Cx a mensagem TRY_OTHER_SERVER_OR_LATER. 
   1. Em outras palavras, o cliente NUNCA deverá obter um value anterior ao que já viu. Além do value, devolva para o cliente o timestamp (do servidor S) associado a Kx


__Observações:__
* Toda comunicação entre servidores será por TCP e deverá obrigatoriamente transferir uma classe Mensagem criada por você.
* Considere que os servidores não vão sair do sistema, não vão morrer e não haverá troca de líder.
* Considere que o líder somente executará um PUT por vez, ou seja, não há requisições PUT concorrentes.


## 4. Mensagens (prints) apresentadas na console

Na console de `cada cliente` deverão ser apresentadas “exatamente” (nem mais nem menos) as seguintes informações:

* Menu interativo (por console) que permita realizar a escolha somente das funções INIT, PUT e GET.
  * * No caso do INIT, realize a inicialização do cliente.
  * * Envio da requisição PUT, capturando do teclado as informações necessárias. 
  * * Envio da requisição GET, capturando do teclado as informações necessária

* Quando receber o PUT_OK, print “PUT_OK key: [key] value [value] timestamp [timestamp] realizada no servidor [IP:porta]”. Substitua a informação entre os parênteses com as reais.

* Quando receber a resposta do GET, print “GET key: [key] value: [valor devolvido pelo servidor] obtido do servidor [IP:porta], meu timestamp [timestamp_do_cliente] e do servidor [timestamp_do_servidor]”

Na console de `cada servidor` deverão ser apresentadas “exatamente” (nem mais nem menos) as seguintes informações:

* Quando receber o PUT:
  * * Se for o líder, print “Cliente [IP]:[porta] PUT key:[key] value:[value].
  * * Se não for o líder, print “Encaminhando PUT key:[key] value:[value]”.
  
* Quando receber o REPLICATION, print “REPLICATION key:[key] value:[value] ts:[timestamp].
  
* Quando o líder receber o REPLICATION_OK de todos, print “Enviando PUT_OK ao Cliente [IP]:[porta] da key:[key] ts:[timestamp_do_servidor].

* Quando receber o GET, print “Cliente [IP]:[porta] GET key:[key] ts:[timestamp]. Meu ts é [timestamp_da_key], portanto devolvendo [valor ou erro]”.


### Perguntas Frequentes:

**Como faço para enviar o objeto Mensagem em vez do String?**
Existem algumas formas. Você pode serializar o objeto (não recomendo) ou usar o formato JSON (recomendo). JSON permite transformar um objeto Python a uma String e vice-versa. Caso queira usar o JSON, use o módulo json. Exemplo de uso: https://pythonexamples.org/convert-python-class-object- to-json/.

**2. Tenho a estrutura no servidor e criei as Threads que atendem os Peers. Estou tendo problemas de concorrência ao atualizar a estrutura. Como resolvo isso?**
Existem diversas formas, como o uso de locks e semáforos, por exemplo.