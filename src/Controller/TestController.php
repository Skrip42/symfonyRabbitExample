<?php
namespace App\Controller;

//юзаем библиотечку "composer require enqueue/amqp-lib"
//я пошерстил и почитал всякое, говорят она самая адекватная
use Enqueue\AmqpLib\AmqpConnectionFactory;
use Interop\Amqp\AmqpTopic;
use Interop\Amqp\AmqpQueue;
use Interop\Amqp\Impl\AmqpBind;
use Interop\Queue\Message;
use Interop\Queue\Consumer;

use Symfony\Bundle\FrameworkBundle\Controller\AbstractController;
use Symfony\Component\Routing\Annotation\Route;

/**
 * ну, это все должно быть в сервисе,
 * но мне лень для тестов пилить, потому вот атк вот:)
 *
 * @Route("/test", name="test")
 */
class TestController extends AbstractController
{
    /**
     * Пример отправки сообещния
     *
     * @Route("/set", name="_set")
     */
    public function setMessage()
    {
        //конектимся к ребиту
        //при установке пакета, мне дредложили определить эти параметры в .env
        //но на практике он всеравно ломился на localhost, поэтому сюда
        $factory = new AmqpConnectionFactory('amqp://test:123@10.1.19.242:5672/%2f');
        $context = $factory->createContext(); //типа контекст, через него работаем с рэбитом

        //создаем очередь, в принципе этот шаг при отправке сообщения можно опустиь
        //при условии что очередь уже создана (например через админку)
        $queue = $context->createQueue('test_queue.v1'); //в имени очереди интернеты советуют писать версию очереди
                                                         //т.к. в случае изменения параметров, очередь с уже существующим именем
                                                         //тупо проигнорится и не создастся
        $queue->addFlag(AmqpQueue::FLAG_DURABLE); //параметр который заставляет ребит дампить очередь на диск
                                                  //чтобы не терять данные при перезапуске сервиса например
        $context->declareQueue($queue);

        //создаем эндпоинт (да, эту операцию советуют делать каждый раз, дубли рабит не создает)
        //в принципе можно не создавать эндпоинты и пользоватся только очередями
        $expoint = $context->createTopic('test_topic.v1');
        $expoint->setType(AmqpTopic::TYPE_FANOUT); //там несколько типов эндпоинтов есть с разным распределением по очередям,
                                                   //с этим уже в доку, в подавляющем большинстве не нужно
        $context->declareTopic($expoint);

        //привязываем очередь к эндпоинту, опять атки можно опустить если все сделать зарание
        $context->bind(new AmqpBind($expoint, $queue));

        $producer = $context->createProducer(); //обьект-отправлятель

        //отправка сообщения (любая строка)
        $producer->send($expoint, $context->createMessage('this is test message'));
        //альтернативный вариант с отправкой сразу в очередь
        $producer->send($queue, $context->createMessage('this is test message'));
        return $this->json('success');
    }

    /**
     * Получить сообщение
     *
     * @Route("/get", name="_get")
     */
    public function getMessage()
    {
        //сдесь все точно так же как и при отправке сообщения
        $factory = new AmqpConnectionFactory('amqp://test:123@10.1.19.242:5672/%2f');
        $context = $factory->createContext();

        //на этот раз очередь создаем обязательно
        $queue = $context->createQueue('test_queue.v1');
        $queue->addFlag(AmqpQueue::FLAG_DURABLE);
        $context->declareQueue($queue);

        $consumer = $context->createConsumer($queue); //создлаем прослушивателя
        $message = $consumer->receive(); //забираем если есть чего
        $consumer->acknowledge($message); //рапартуем что забрали и закончили обрабатывать

        return $this->json($message->getBody()); //через getBody получаем текст сообщения
    }

    /**
     * Подписка на сообщения (если хотим повиснуь в памяти и слушать)
     *
     * @Route("/sub", name="_sub")
     */
    public function subscribe()
    {
        //все аналогично get
        $factory = new AmqpConnectionFactory('amqp://test:123@10.1.19.242:5672/%2f');
        $context = $factory->createContext();

        //все аналогично get
        $queue = $context->createQueue('test_queue.v1');
        $queue->addFlag(AmqpQueue::FLAG_DURABLE);
        $context->declareQueue($queue);

        $consumer = $context->createConsumer($queue); //создаем слушителя
        $subscriptionConsumer = $context->createSubscriptionConsumer(); //создаем "подписку" слушетеля
        $subscriptionConsumer->subscribe( //подписываем слушетеля
            $consumer,
            function (Message $message, Consumer $consumer) { //калбек при получении сообщения
                sleep(1);
                $consumer->acknowledge($message); //рапартуем что получили и обработали
                return true;
            }
        );
        $subscriptionConsumer->consume(5000); //говорим "слушай че там 5 секунд"
        return $this->json('true');
    }
}
