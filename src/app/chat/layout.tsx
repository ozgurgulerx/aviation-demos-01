import { Header } from "@/components/layout/header";
import { Footer } from "@/components/layout/footer";

export default function ChatLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <div className="flex flex-col h-screen bg-background">
      <Header />
      <main className="flex-1 min-h-0 overflow-hidden">{children}</main>
      <Footer />
    </div>
  );
}
