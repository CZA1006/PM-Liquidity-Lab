import MarketView from "@/components/MarketView";

export default async function MarketPage({
  params,
}: {
  params: Promise<{ slug: string }>;
}) {
  const { slug } = await params;
  return (
    <main className="space-y-4">
      <MarketView slug={slug} />
    </main>
  );
}
