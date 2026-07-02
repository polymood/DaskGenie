"use client";

import { PostMortem } from "@/components/PostMortem";

export default function PostMortemPage({ params }: { params: { id: string } }) {
  return <PostMortem runId={params.id} />;
}
