import {
  publish,
  subscriberInitializer,
  EventId,
  Topic,
  Domain,
} from "./publisher";

const testing = async () => {
  try {
    const orderSubscriber = subscriberInitializer(Domain.ORDER);
    const paymentSubscriber = subscriberInitializer(Domain.PAYMENT);
    const inventorySubscriber = subscriberInitializer(Domain.INVENTORY);
    const deliverySubscriber = subscriberInitializer(Domain.DELIVERY);
    const notificationSubscriber = subscriberInitializer(Domain.NOTIFICATION);

    await Promise.all([
      paymentSubscriber(
        Topic.ORDER_CREATED,
        async (parentId: string, content: any) => {
          return new Promise((resolve, reject) => {
            //결제 데이터 생성 & PG사 결제 시도
            console.log("... Payment service consuming ORDER_CREATED");
            setTimeout(async () => {
              await publish(parentId, Topic.PAYMENT_SUCCESS, {
                paymentId: EventId(),
              });
              resolve(null);
            }, 1000);
          });
        }
      ),
      inventorySubscriber(
        Topic.ORDER_CREATED,
        async (parentId: string, content: any) => {
          return new Promise((resolve, reject) => {
            //재고 서비스에서 재고 확보
            console.log("... Inventory service consuming ORDER_CREATED");
            setTimeout(async () => {
              await publish(parentId, Topic.PRODUCTS_READY, {
                inventoryId: EventId(),
              });
              resolve(null);
            }, 1000);
          });
        }
      ),
      orderSubscriber(
        Topic.PAYMENT_SUCCESS,
        async (parentId: string, content: any) => {
          return new Promise((resolve, reject) => {
            //결제 확인이 되면 주문 상태를 배송준비로 변경
            console.log("... Order service consuming PAYMENT_SUCCESS");
            setTimeout(async () => {
              await publish(parentId, Topic.ORDER_SHIPPING_READY, {
                status: "SHIPPING_READY",
              });
              resolve(null);
            }, 1000);
          });
        }
      ),
      deliverySubscriber(
        Topic.ORDER_SHIPPING_READY,
        async (parentId: string, content: any) => {
          return new Promise((resolve, reject) => {
            //배송데이터 생성 & 우체국 API 호출해서 송장 생성
            console.log("... Delivery service consuming ORDER_SHIPPING_READY");
            setTimeout(async () => {
              await publish(parentId, Topic.INVOICE_READY, {
                postId: EventId(),
              });
              resolve(null);
            }, 1000);
          });
        }
      ),
      notificationSubscriber(
        Topic.ORDER_SHIPPING_READY,
        async (parentId: string, content: any) => {
          return new Promise((resolve, reject) => {
            // 출고 준비되었다고 유저에게 플친/메시지 전송
            console.log(
              "... Notification service consuming ORDER_SHIPPING_READY"
            );
            setTimeout(async () => {
              await publish(parentId, Topic.PAY_SUCCESS_MESSAGE_DELIVERED, {
                postId: EventId(),
              });
              resolve(null);
            }, 1000);
          });
        }
      ),
    ]);

    // 주문 생성 API 호출로 첫 이벤트 발생
    // console.log("... Received POST /order");
    // setTimeout(() => {
    //   publish(EventId(), Topic.ORDER_CREATED, {
    //     userId: EventId(),
    //     products: [
    //       { name: "Tesla model3", qty: 1 },
    //       { name: "Cybertruck", qty: 2 },
    //     ],
    //   });
    // }, 1500);
  } catch (err) {
    console.log(err);
  }
};

testing();
const offsets = {
  type1: {
    acknowledged: 1,
  },
};
