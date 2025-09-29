// supabase/functions/shopify-orders/index.ts
import "jsr:@supabase/functions-js/edge-runtime.d.ts";
import { createClient } from "jsr:@supabase/supabase-js@2";
import { crypto } from "jsr:@std/crypto/crypto";

// === BẮT ĐẦU THAY ĐỔI ===
// Đổi tên biến để không bắt đầu bằng "SUPABASE_"
const PROJECT_URL = Deno.env.get("PROJECT_URL")!;
const PROJECT_SERVICE_ROLE_KEY = Deno.env.get("PROJECT_SERVICE_ROLE_KEY")!;
const SHOPIFY_WEBHOOK_SECRET = Deno.env.get("SHOPIFY_WEBHOOK_SECRET")!;

// Khởi tạo Supabase client với các biến đã đổi tên
const supabase = createClient(PROJECT_URL, PROJECT_SERVICE_ROLE_KEY, { 
  auth: { persistSession: false } 
});
// === KẾT THÚC THAY ĐỔI ===


// Hàm xác thực chữ ký HMAC từ Shopify
async function verifyHmac(rawBody: string, hmacHeader: string | null) {
  if (!hmacHeader) return false;
  const keyData = new TextEncoder().encode(SHOPIFY_WEBHOOK_SECRET);
  const msgData = new TextEncoder().encode(rawBody);

  const key = await crypto.subtle.importKey("raw", keyData, { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
  const signature = await crypto.subtle.sign("HMAC", key, msgData);
  
  const base64Signature = btoa(String.fromCharCode(...new Uint8Array(signature)));
  
  return base64Signature === hmacHeader;
}

// Hàm chính xử lý request đến
Deno.serve(async (req) => {
  const topic = req.headers.get("X-Shopify-Topic") || "";
  const hmac = req.headers.get("X-Shopify-Hmac-Sha256");
  const rawBody = await req.text();

  if (!(await verifyHmac(rawBody, hmac))) {
    console.error("Invalid HMAC signature.");
    return new Response("Invalid HMAC signature.", { status: 401 });
  }

  if (topic !== "orders/create") {
    console.log(`Ignoring topic: ${topic}`);
    return new Response("Webhook received, but topic is ignored.", { status: 200 });
  }

  try {
    const order = JSON.parse(rawBody);
    const orderId = String(order.id);
    const createdAt = order.created_at; 
    const subtotal = parseFloat(order.subtotal_price || "0.0");

    const { data, error } = await supabase.from("sales_events").insert({
      order_id: orderId,
      product_title: order.line_items?.[0]?.title ?? "Shopify Order",
      revenue: subtotal,
      created_at: createdAt
    }).select();

    if (error) {
      if (error.code === '23505') { 
        console.log(`Duplicate order ignored: ${orderId}`);
        return new Response("Duplicate order, ignored.", { status: 200 });
      }
      console.error("Supabase insert error:", error.message);
      return new Response(error.message, { status: 500 });
    }
    
    console.log(`Successfully processed order: ${orderId}`);
    return new Response("Webhook processed successfully.", { status: 201 });

  } catch (e) {
    console.error("Error parsing JSON or processing webhook:", e);
    return new Response("Invalid request body.", { status: 400 });
  }
});