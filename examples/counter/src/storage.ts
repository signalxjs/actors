import { fileStorage } from '@sigx/actors/node';

/** Dev storage — one JSON file per actor under .actors/ (cat-able). */
export const storage = fileStorage({ dir: '.actors' });
