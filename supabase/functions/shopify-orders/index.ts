// supabase/functions/shopify-orders/index.ts
import "jsr:@supabase/functions-js/edge-runtime.d.ts";
import { createClient } from "jsr:@supabase/supabase-js@2";
import { crypto } from "jsr:@std/crypto/crypto";

const PROJECT_URL = Deno.env.get("PROJECT_URL")!;
const PROJECT_SERVICE_ROLE_KEY = Deno.env.get("PROJECT_SERVICE_ROLE_KEY")!;

// Khởi tạo Supabase client
const supabase = createClient(PROJECT_URL, PROJECT_SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

// Hàm xác thực chữ ký HMAC từ Shopify, giờ đây nhận secret làm tham số
async function verifyHmac(rawBody: string, hmacHeader: string | null, secret: string) {
  if (!hmacHeader || !secret) return false;
  const keyData = new TextEncoder().encode(secret);
  const msgData = new TextEncoder().encode(rawBody);

  const key = await crypto.subtle.importKey("raw", keyData, { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
  const signature = await crypto.subtle.sign("HMAC", key, msgData);
  
  const base64Signature = btoa(String.fromCharCode(...new Uint8Array(signature)));
  
  return base64Signature === hmacHeader;
}

// Bảng map tên sản phẩm sang biểu tượng (tạm thời, bạn có thể mở rộng sau)
const PRODUCT_SYMBOL_MAPPING: { [key: string]: string } = {
  "128 Hz Healing Instrument": "🌱",
  "Hidden Camera Detector": "📹",
  "Resistance Breathing Necklace": "🌿"
};

function getProductSymbol(productTitle: string): string {
  for (const keyword in PRODUCT_SYMBOL_MAPPING) {
    if (productTitle.toLowerCase().includes(keyword.toLowerCase())) {
      return PRODUCT_SYMBOL_MAPPING[keyword];
    }
  }
  return "🛒"; // Biểu tượng mặc định
}

// Hàm chính xử lý request đến
Deno.serve(async (req) => {
  const url = new URL(req.url);
  const storeId = url.searchParams.get("store_id");

  if (!storeId) {
    console.error("Missing 'store_id' in webhook URL.");
    return new Response("Missing store_id parameter.", { status: 400 });
  }

  // Lấy thông tin cửa hàng từ database dựa trên store_id
  const { data: storeInfo, error: dbError } = await supabase
    .from("shopify_stores")
    .select("webhook_secret, store_name")
    .eq("store_id", storeId)
    .single();

  if (dbError || !storeInfo) {
    console.error(`Store not found or DB error for store_id '${storeId}':`, dbError?.message);
    return new Response("Store configuration not found.", { status: 404 });
  }

  const { webhook_secret: webhookSecret, store_name: storeName } = storeInfo;
  
  const topic = req.headers.get("X-Shopify-Topic") || "";
  const hmac = req.headers.get("X-Shopify-Hmac-Sha256");
  const rawBody = await req.text();

  // Xác thực HMAC với secret của đúng cửa hàng
  if (!(await verifyHmac(rawBody, hmac, webhookSecret))) {
    console.error(`Invalid HMAC signature for store: ${storeId}`);
    return new Response("Invalid HMAC signature.", { status: 401 });
  }

  if (topic !== "orders/create") {
    console.log(`Ignoring topic '${topic}' for store '${storeId}'`);
    return new Response("Webhook received, but topic is ignored.", { status: 200 });
  }

  try {
    const order = JSON.parse(rawBody);
    const orderId = String(order.id);
    const createdAt = order.created_at;
    const subtotal = parseFloat(order.subtotal_price || "0.0");
    const productTitle = order.line_items?.[0]?.title ?? "Shopify Order";
    const productSymbol = getProductSymbol(productTitle);
    
    // Khi insert, thêm cả store_id, store_name và product_symbol
    const { error: insertError } = await supabase.from("sales_events").insert({
      order_id: orderId,
      product_title: productTitle,
      revenue: subtotal,
      created_at: createdAt,
      store_id: storeId, // <-- Dữ liệu mới
      store_name: storeName, // <-- Dữ liệu mới
      product_symbol: productSymbol // <-- Dữ liệu mới
    });

    if (insertError) {
      if (insertError.code === '23505') { 
        console.log(`Duplicate order ignored for store '${storeId}': ${orderId}`);
        return new Response("Duplicate order, ignored.", { status: 200 });
      }
      console.error("Supabase insert error:", insertError.message);
      return new Response(insertError.message, { status: 500 });
    }
    
    console.log(`Successfully processed order '${orderId}' from store '${storeId}'`);
    return new Response("Webhook processed successfully.", { status: 201 });

  } catch (e) {
    console.error("Error parsing JSON or processing webhook:", e.message);
    return new Response("Invalid request body.", { status: 400 });
  }
});
